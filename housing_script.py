# --- IMPORTS ---
import arcpy
import os
import csv
import re
from datetime import datetime
from collections import defaultdict
# Settings
RESET = True
GDB_PATH = r"C:\Users\jperlis\OneDrive - Brown University\Documents\ArcGIS\Projects\DataDesk_housing_automatedTEST1\DataDesk_housing_automatedTEST1.gdb"
# Folder with data:
SNAPSHOT_FOLDER = r"C:\Users\jperlis\Downloads\Data"
SNAPSHOT_PREFIX = "spring_room_selection_"
SNAPSHOT_YEAR = 2025
LOOKUP_CSV = r"C:\Users\jperlis\Downloads\bdh_datadesk_lottery_ref_1.csv"
DORM_POLYGONS = "brown_basemap"
TIME_SERIES_FC = "Dorm_RoomAvailability_TimeSeries"
INVENTORY_TABLE = "Dorm_RoomInventory"
# ---- FULL PATHS INSIDE GDB ----
TIME_SERIES_FC_PATH = os.path.join(GDB_PATH, TIME_SERIES_FC)
INVENTORY_TABLE_PATH = os.path.join(GDB_PATH, INVENTORY_TABLE)

# Filters:

# Base genders in the raw data
BASE_GENDERS = ("COED", "MALE", "FEMALE")
# Dropdown gender groupings
GENDER_GROUPS = ("COED", "COEDMALE", "COEDFEMALE", "ALL")
# Dropdown room-size options
SIZE_OPTIONS = ("ALL", 1, 2, 3, 4, 5, 9)

# Field name helpers

def size_label(size_opt):
    return "ALL" if size_opt == "ALL" else str(int(size_opt))
def avail_field(g, s):
    return f"Avail_{g}_{size_label(s)}"
def total_field(g, s):
    return f"Total_{g}_{size_label(s)}"
def pct_field(g, s):
    return f"Pct_{g}_{size_label(s)}"
def all_slice_fields():
    """
    Returns the 84 fields in a stable order:
      Avail_*, Total_*, Pct_* for each (gender_group, size_option)
    """
    fields = []
    for g in GENDER_GROUPS:
        for s in SIZE_OPTIONS:
            fields.append(avail_field(g, s))
            fields.append(total_field(g, s))
            fields.append(pct_field(g, s))
    return fields
def all_total_fields():
    """
    Returns the 28 Total_* fields for the inventory table.
    """
    fields = []
    for g in GENDER_GROUPS:
        for s in SIZE_OPTIONS:
            fields.append(total_field(g, s))
    return fields

# Helper functions

def normalize_name(name):
    return (
        (name or "")
        .upper()
        .replace(".", "")
        .replace("#", "")
        .replace("-", " ")
        .replace("  ", " ")
        .strip()
    )
def load_building_lookup(csv_path):
    lookup = {}
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lottery_name = normalize_name(row["Lottery_sheet_name"])
            building_id = int(row["Building_ID"])
            lookup[lottery_name] = building_id
    return lookup
def load_building_id_to_name(csv_path):
    """
    Returns dict: {Building_ID (int): Lottery_sheet_name (original casing from CSV)}
    """
    out = {}
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bid = int(row["Building_ID"])
            out[bid] = row["Lottery_sheet_name"].strip()
    return out
def parse_snapshot_time_from_filename(filename, year):
    """
    Expected filename pattern:
        spring_room_selection_<month>_<day>_<HHMM>.csv
    Example:
        spring_room_selection_4_8_0900.csv  -> April 8, 09:00
    Returns: datetime
    """
    fn = os.path.basename(filename)
    pattern = r"^" + re.escape(SNAPSHOT_PREFIX) + r"(\d{1,2})_(\d{1,2})_(\d{4})\.csv$"
    m = re.match(pattern, fn)
    if not m:
        raise ValueError(f"Filename does not match expected pattern: {fn}")
    month = int(m.group(1))
    day = int(m.group(2))
    hhmm = m.group(3)
    hour = int(hhmm[:2])
    minute = int(hhmm[2:])
    return datetime(year, month, day, hour, minute, 0)
def get_snapshot_files_with_times(folder):
    """
    Returns list of (snapshot_time, full_path) sorted by snapshot_time.
    """
    snapshots = []
    for fn in os.listdir(folder):
        if not fn.startswith(SNAPSHOT_PREFIX) or not fn.endswith(".csv"):
            continue
        full_path = os.path.join(folder, fn)
        try:
            t = parse_snapshot_time_from_filename(fn, SNAPSHOT_YEAR)
        except ValueError:
            continue
        snapshots.append((t, full_path))
    snapshots.sort(key=lambda x: x[0])
    return snapshots
def map_base_gender(raw_gender):
    """
    Maps raw Room Gender values to base categories:
      COED includes CoEd + DynamicGender
      MALE includes Male
      FEMALE includes Female
    Returns None if not recognized.
    """
    g = (raw_gender or "").strip()
    if g in ("CoEd", "DynamicGender"):
        return "COED"
    if g == "Male":
        return "MALE"
    if g == "Female":
        return "FEMALE"
    return None
def create_inventory_table():
    """
    Inventory table stores baseline totals for each (gender_group, size_option) slice.
    """
    if not arcpy.Exists(INVENTORY_TABLE_PATH):
        arcpy.management.CreateTable(GDB_PATH, INVENTORY_TABLE)
        arcpy.management.AddField(INVENTORY_TABLE_PATH, "Building", "LONG")
    # Ensure required total fields exist
    existing = {f.name for f in arcpy.ListFields(INVENTORY_TABLE_PATH)}
    for fld in all_total_fields():
        if fld not in existing:
            arcpy.management.AddField(INVENTORY_TABLE_PATH, fld, "LONG")
def create_time_series_fc():
    """
    Time-series FC stores 84 fields for each (gender_group, size_option) slice:
      Avail_*, Total_*, Pct_*
    """
    if not arcpy.Exists(TIME_SERIES_FC_PATH):
        arcpy.management.CopyFeatures(DORM_POLYGONS, TIME_SERIES_FC_PATH)
        arcpy.management.AddField(TIME_SERIES_FC_PATH, "Snapshot_Time", "DATE")
    # Ensure required fields exist
    existing = {f.name for f in arcpy.ListFields(TIME_SERIES_FC_PATH)}
    if "Building_Name" not in existing:
        arcpy.management.AddField(TIME_SERIES_FC_PATH, "Building_Name", "TEXT", field_length=100)
        existing.add("Building_Name")
    # Keep legacy fields if you want; not required for the new system
    legacy_fields = [
        ("Rooms_Available", "LONG"),
        ("Total_Rooms", "LONG"),
        ("Percent_Available", "DOUBLE"),
    ]
    for fname, ftype in legacy_fields:
        if fname not in existing:
            arcpy.management.AddField(TIME_SERIES_FC_PATH, fname, ftype)
    for g in GENDER_GROUPS:
        for s in SIZE_OPTIONS:
            a = avail_field(g, s)
            t = total_field(g, s)
            p = pct_field(g, s)
            if a not in existing:
                arcpy.management.AddField(TIME_SERIES_FC_PATH, a, "LONG")
            if t not in existing:
                arcpy.management.AddField(TIME_SERIES_FC_PATH, t, "LONG")
            if p not in existing:
                arcpy.management.AddField(TIME_SERIES_FC_PATH, p, "DOUBLE")
def get_max_existing_timestamp():
    if not arcpy.Exists(TIME_SERIES_FC_PATH):
        return None
    max_time = None
    with arcpy.da.SearchCursor(TIME_SERIES_FC_PATH, ["Snapshot_Time"]) as cursor:
        for (t,) in cursor:
            if t and (max_time is None or t > max_time):
                max_time = t
    return max_time

#-------------------------------------
# Aggregation
### IMPORTANT
def process_snapshot(snapshot_csv, building_lookup):
    """
    Computes AVAILABLE fully-available room/suite counts for each building across:
      - base genders: COED/MALE/FEMALE
      - capacities (room/suite size): integer capacity
    Returns:
      avail_counts[building_id][base_gender][capacity] = count_of_fully_available_rooms
      avail_counts[building_id][base_gender]["ALL"] = sum across all capacities
    """
    # Track each unique room instance by: (building_id, room_key, base_gender)
    # Each holds capacity and available_beds count.
    rooms = {}
    counted_rooms = set()  # For special case "GREG A 125" block
    with open(snapshot_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            profile = row.get("Room Profile")
            if profile != "25-26 Spring Selection (Room)":
                continue
            building_name = normalize_name(row.get("Building"))
            if building_name not in building_lookup:
                print("NOT MATCHED:", building_name)
                continue
            building_id = building_lookup[building_name]
            base_gender = map_base_gender(row.get("Room Gender"))
            if base_gender is None:
                # Skip unrecognized gender categories
                continue
            room_type = (row.get("Room Type") or "").strip()
            room_str = (row.get("Room") or "")
            # --- Grad Center rooms ---
            if "GRAD CENTER" in building_name:
                room_id = (row.get("Room") or "").strip()
                if not room_id:
                    continue
                capacity = 1
                key = (building_id, room_id, base_gender)
                if key not in rooms:
                    rooms[key] = {"capacity": capacity, "available_beds": 1}
                continue
            # --- Special suite case: GREG A 125 ---
            if "GREG A 125" in room_str:
                if "GREG A 125" not in counted_rooms:
                    capacity = 9
                    key = (building_id, "GREG A 125", base_gender)
                    rooms[key] = {"capacity": capacity, "available_beds": capacity}
                    for i in range(125, 133):
                        counted_rooms.add(f"GREG A {i}")
                continue
            # --- Regular suites ---
            if "Suite" in room_type:
                suite_id = (row.get("Suite") or "").strip()  # **Important: use Suite column**
                suite_size_raw = (row.get("Suite Size (if applicable)") or "").strip()
                if (not suite_id) or (suite_size_raw == "") or (suite_size_raw.upper() in ("NA", "N/A", "-", "NONE")):
                    print(f"Missing suite info for {building_name} {row.get('Room')}")
                    continue
                try:
                    capacity = int(float(suite_size_raw))
                except ValueError:
                    print(f"Bad suite size for {building_name} {row.get('Room')}: {suite_size_raw!r}")
                    continue
                key = (building_id, suite_id, base_gender)
                if key not in rooms:
                    rooms[key] = {"capacity": capacity, "available_beds": 1}
                else:
                    rooms[key]["available_beds"] += 1
                continue
            # --- Standard rooms ---
            room_id = (row.get("Suite") or "").strip()
            if not room_id:
                # If Suite is ever blank, you can decide to skip or fallback to Room.
                # Keeping strict to your current assumption: Suite is the ID.
                continue
            if "Single" in room_type:
                capacity = 1
            elif "Double" in room_type:
                capacity = 2
            elif "Triple" in room_type:
                capacity = 3
            elif "Quad" in room_type:
                capacity = 4
            else:
                print(f"Unknown room type for {building_name} {room_id}: {room_type}")
                continue
            key = (building_id, room_id, base_gender)
            if key not in rooms:
                rooms[key] = {"capacity": capacity, "available_beds": 1}
            else:
                rooms[key]["available_beds"] += 1
    # Aggregate fully-available rooms by building/base_gender/capacity
    avail_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for (building_id, _room_id, base_gender), data in rooms.items():
        cap = data["capacity"]
        if data["available_beds"] == cap:
            avail_counts[building_id][base_gender][cap] += 1
            avail_counts[building_id][base_gender]["ALL"] += 1
    # Ensure all buildings appear (even if zero)
    for _bname, bid in building_lookup.items():
        if bid not in avail_counts:
            # initialize empty
            _ = avail_counts[bid]
    return avail_counts
def totals_from_snapshot(snapshot_csv, building_lookup):
    """
    Computes TOTAL room/suite counts for each building across:
      - base genders: COED/MALE/FEMALE
      - capacities (room/suite size): integer capacity
    This counts unique rooms/suites regardless of availability status.
    Returns:
      total_counts[building_id][base_gender][capacity] = total_rooms
      total_counts[building_id][base_gender]["ALL"] = sum across all capacities
    """
    seen = set()
    counted_rooms = set()
    # We store (building_id, room_id, base_gender) -> capacity
    room_caps = {}
    with open(snapshot_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            profile = row.get("Room Profile")
            if profile != "25-26 Spring Selection (Room)":
                continue
            building_name = normalize_name(row.get("Building"))
            if building_name not in building_lookup:
                continue
            building_id = building_lookup[building_name]
            base_gender = map_base_gender(row.get("Room Gender"))
            if base_gender is None:
                continue
            room_type = (row.get("Room Type") or "").strip()
            room_str = (row.get("Room") or "")
            # Grad Center
            if "GRAD CENTER" in building_name:
                room_id = (row.get("Room") or "").strip()
                if not room_id:
                    continue
                capacity = 1
                key = (building_id, room_id, base_gender)
                if key not in seen:
                    seen.add(key)
                    room_caps[key] = capacity
                continue
            # Special GREG A 125
            if "GREG A 125" in room_str:
                if "GREG A 125" not in counted_rooms:
                    capacity = 9
                    key = (building_id, "GREG A 125", base_gender)
                    if key not in seen:
                        seen.add(key)
                        room_caps[key] = capacity
                    for i in range(125, 133):
                        counted_rooms.add(f"GREG A {i}")
                continue
            # Suites
            if "Suite" in room_type:
                suite_id = (row.get("Suite") or "").strip()
                suite_size_raw = (row.get("Suite Size (if applicable)") or "").strip()
                if (not suite_id) or (suite_size_raw == "") or (suite_size_raw.upper() in ("NA", "N/A", "-", "NONE")):
                    continue
                try:
                    capacity = int(float(suite_size_raw))
                except ValueError:
                    continue
                key = (building_id, suite_id, base_gender)
                if key not in seen:
                    seen.add(key)
                    room_caps[key] = capacity
                continue
            # Standard rooms
            room_id = (row.get("Suite") or "").strip()
            if not room_id:
                continue
            if "Single" in room_type:
                capacity = 1
            elif "Double" in room_type:
                capacity = 2
            elif "Triple" in room_type:
                capacity = 3
            elif "Quad" in room_type:
                capacity = 4
            else:
                continue
            key = (building_id, room_id, base_gender)
            if key not in seen:
                seen.add(key)
                room_caps[key] = capacity
    total_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for (building_id, _room_id, base_gender), cap in room_caps.items():
        total_counts[building_id][base_gender][cap] += 1
        total_counts[building_id][base_gender]["ALL"] += 1
    for _bname, bid in building_lookup.items():
        if bid not in total_counts:
            _ = total_counts[bid]
    return total_counts
def aggregate_to_groups(counts_by_base):
    """
    Converts base counts (COED/MALE/FEMALE) into the 4 dropdown gender groups:
      COED, COEDMALE, COEDFEMALE, ALL
    Input:
      counts_by_base[building_id][base_gender][cap_or_ALL] = count
    Output:
      counts_by_group[building_id][group][cap_or_ALL] = count
    """
    out = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for bid, by_base_gender in counts_by_base.items():
        # helper to get count safely
        def get(bg, cap):
            return by_base_gender.get(bg, {}).get(cap, 0)
        # Determine which capacity keys exist (include "ALL" plus any integers)
        cap_keys = set()
        for bg in BASE_GENDERS:
            cap_keys.update(by_base_gender.get(bg, {}).keys())
        if "ALL" not in cap_keys:
            cap_keys.add("ALL")
        for cap in cap_keys:
            coed = get("COED", cap)
            male = get("MALE", cap)
            female = get("FEMALE", cap)
            out[bid]["COED"][cap] = coed
            out[bid]["COEDMALE"][cap] = coed + male
            out[bid]["COEDFEMALE"][cap] = coed + female
            out[bid]["ALL"][cap] = coed + male + female
    return out
def slice_value(counts_by_group, bid, group, size_opt):
    """
    Returns the count for a (group, size_opt) slice.
    size_opt can be "ALL" or an int capacity.
    For size_opt="ALL", uses the precomputed "ALL" cap bucket.
    For numeric size_opt, uses that exact capacity bucket.
    """
    if size_opt == "ALL":
        return counts_by_group.get(bid, {}).get(group, {}).get("ALL", 0)
    return counts_by_group.get(bid, {}).get(group, {}).get(int(size_opt), 0)
# ==========================================================
# --- MAIN WORKFLOW ---
# ==========================================================
def main():
    # --- ARCPY SETUP ---
    arcpy.env.workspace = GDB_PATH
    arcpy.ClearWorkspaceCache_management()
    # --- RESET OUTPUTS IF REQUESTED ---
    if RESET:
        if arcpy.Exists(TIME_SERIES_FC_PATH):
            arcpy.management.Delete(TIME_SERIES_FC_PATH)
            print("Deleted old time-series feature class")
        if arcpy.Exists(INVENTORY_TABLE_PATH):
            arcpy.management.Delete(INVENTORY_TABLE_PATH)
            print("Deleted old inventory table")
    # --- DISCOVER SNAPSHOTS ---
    snapshots = get_snapshot_files_with_times(SNAPSHOT_FOLDER)
    if len(snapshots) == 0:
        raise FileNotFoundError(
            f"No snapshot CSVs found in {SNAPSHOT_FOLDER} matching {SNAPSHOT_PREFIX}<m>_<d>_<HHMM>.csv"
        )
    # --- LOAD LOOKUP ---
    building_lookup = load_building_lookup(LOOKUP_CSV)
    building_id_to_name = load_building_id_to_name(LOOKUP_CSV)
    # --- CREATE OUTPUTS IF NEEDED ---
    create_inventory_table()
    create_time_series_fc()
    # --- LOAD INVENTORY TOTALS (28 total fields) ---
    total_fields = all_total_fields()
    inv_fields = ["Building"] + total_fields
    totals_by_building = {}  # bid -> {Total_*: value}
    with arcpy.da.SearchCursor(INVENTORY_TABLE_PATH, inv_fields) as cursor:
        for row in cursor:
            bid = row[0]
            totals_by_building[bid] = {}
            for i, fld in enumerate(total_fields, start=1):
                totals_by_building[bid][fld] = row[i] if row[i] is not None else 0
    # --- INIT INVENTORY TOTALS ON FIRST RUN ---
    if not totals_by_building:
        baseline_time, baseline_csv = snapshots[0]
        # Baseline inventory totals should be defined the same way as "available":
        # count only FULLY available rooms/suites at baseline (ignore partial "figments").
        base_totals = process_snapshot(baseline_csv, building_lookup)
        group_totals = aggregate_to_groups(base_totals)
        with arcpy.da.InsertCursor(INVENTORY_TABLE_PATH, inv_fields) as icur:
            for _bname, bid in building_lookup.items():
                row_vals = [bid]
                totals_by_building[bid] = {}
                for g in GENDER_GROUPS:
                    for s in SIZE_OPTIONS:
                        tot = slice_value(group_totals, bid, g, s)
                        fld = total_field(g, s)
                        totals_by_building[bid][fld] = tot
                        row_vals.append(tot)
                icur.insertRow(row_vals)
        print(f"Inventory initialized from {os.path.basename(baseline_csv)} ({baseline_time})")
    # --- SKIP ALREADY-INGESTED SNAPSHOTS (IF NOT RESET) ---
    max_existing = get_max_existing_timestamp()
    # --- INSERT FIELDS FOR TIME SERIES ---
    # Include geometry + Building + Snapshot_Time, then 84 slice fields.
    slice_fields = all_slice_fields()
    insert_fields = [
        "SHAPE@",
        "Building",
        "Building_name",
        "Snapshot_Time",
        # legacy fields (optional)
        "Rooms_Available",
        "Total_Rooms",
        "Percent_Available",
    ] + slice_fields
    # --- APPEND TIME SERIES FOR EACH SNAPSHOT ---
    for snapshot_time, snapshot_csv in snapshots:
        if max_existing is not None and snapshot_time <= max_existing:
            continue
        base_avail = process_snapshot(snapshot_csv, building_lookup)
        group_avail = aggregate_to_groups(base_avail)
      
        with arcpy.da.InsertCursor(TIME_SERIES_FC_PATH, insert_fields) as icursor:
            with arcpy.da.SearchCursor(DORM_POLYGONS, ["SHAPE@", "Building"]) as dorms:
                for shape, bid in dorms:
                    # If we don't have totals for this building, skip
                    if bid not in totals_by_building:
                        continue
                    # Legacy "ALL / ALL" convenience
                    legacy_avail = slice_value(group_avail, bid, "ALL", "ALL")
                    legacy_total = totals_by_building[bid].get(total_field("ALL", "ALL"), 0)
                    legacy_pct = (legacy_avail / legacy_total * 100.0) if legacy_total > 0 else None
                    bname = building_id_to_name.get(bid, None)
                    row_out = [
                        shape,
                        bid,
                        bname,
                        snapshot_time,
                        legacy_avail,
                        legacy_total,
                        legacy_pct,
                    ]
                    # Fill 84 slice fields
                    for g in GENDER_GROUPS:
                        for s in SIZE_OPTIONS:
                            a = slice_value(group_avail, bid, g, s)
                            t_fld = total_field(g, s)
                            t = totals_by_building[bid].get(t_fld, 0)
                            if t > 0:
                                p = (a / t) * 100.0
                                if p > 100.0:
                                    p = 100.0
                                a_out = a
                            else:
                                # No rooms of this type exist in this building -> store NULL so it can be symbolized gray
                                p = None
                                a_out = None
                            row_out.append(a_out)
                            row_out.append(t)
                            row_out.append(p)
                    icursor.insertRow(row_out)
        print(f"Snapshot appended for {snapshot_time} from {os.path.basename(snapshot_csv)}")
# ==========================================================
# --- RUN ---
# ==========================================================
if __name__ == "__main__":
    main()
