from __future__ import annotations

import io
import re
import unicodedata
import zipfile
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import streamlit as st


BASE_DIR = Path(__file__).resolve().parent
SELLERS_FILE_CANDIDATES = [
    "aktive_verkaeufer.txt",
    "aktive Verkäufer.txt",
    "aktive Verkäufer.txt",
]

SUCCESSFUL_CONTACT_TYPES = {
    "Online Termin vereinbart",
    "vor Ort Termin vereinbart",
    "Telefon Termin vereinbart",
    "Verkaufsgespräch durchgeführt",
    "Telefonat",
    "Telefonat mit Entscheidungsträger",
    "Kaltbesuch",
    "Servicetermin",
    "Termin vereinbart (neu durch AD)",
    "Termin vereinbart",
    "Auftrag (automatisch erfasst)",
}

ATTEMPT_CONTACT_TYPES = {
    "Telefonisch nicht erreicht",
    "Kunde nicht anwesend - neuen Termin vereinbaren",
    "Kunde hat abgesagt - neuen Termin ausmachen",
    "Kunde nicht anwesend – kein Interesse",
    "Kunde hat abgesagt – kein Interesse",
    "Kunde nicht anwesend ? kein Interesse",
    "Kunde hat abgesagt ? kein Interesse",
    "Termin musste durch Verkauf abgesagt werden",
}

ORDERS_REQUIRED_COLUMNS = {
    "Firma",
    "Kundennummer",
    "Herold-Nummer",
    "Zugewiesen an",
    "Produkt",
    "Abo",
    "Kündigungsdatum",
}

CONTACTS_REQUIRED_COLUMNS = {
    "Datum/Uhrzeit",
    "Mitarbeiter",
    "Kontakt",
    "Kontaktart",
    "Kundennummer",
    "Herold-Nummer (Firma)",
}


@dataclass
class DataSources:
    orders_name: str
    contacts_name: str


def contact_export_range(contacts: pd.DataFrame) -> tuple[pd.Timestamp | pd.NaT, pd.Timestamp | pd.NaT]:
    valid = contacts["contact_datetime"].dropna()
    if valid.empty:
        return pd.NaT, pd.NaT
    return valid.min(), valid.max()


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip().lower().replace("\xa0", " ")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_filename(value: str) -> str:
    text = unicodedata.normalize("NFC", value).strip().lower()
    replacements = {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "ß": "ss",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"[^a-z0-9]+", "", text)
    return text


def resolve_sellers_file() -> Path | None:
    for candidate in SELLERS_FILE_CANDIDATES:
        path = BASE_DIR / candidate
        if path.exists():
            return path

    expected_names = {normalize_filename(candidate) for candidate in SELLERS_FILE_CANDIDATES}
    for path in sorted(BASE_DIR.glob("*.txt")):
        if normalize_filename(path.name) in expected_names:
            return path

    return None


def read_sellers(path: Path | None) -> list[str]:
    if path is None or not path.exists():
        return []
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def find_default_file(prefix: str) -> Path | None:
    candidates = sorted(BASE_DIR.glob(f"{prefix}*"), reverse=True)
    return candidates[0] if candidates else None


def decode_csv_bytes(file_bytes: bytes) -> str:
    for encoding in ("utf-8-sig", "cp1252", "latin1"):
        try:
            return file_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    return file_bytes.decode("latin1", errors="replace")


def extract_if_zipped(file_bytes: bytes, filename: str) -> tuple[bytes, str]:
    if not zipfile.is_zipfile(io.BytesIO(file_bytes)):
        return file_bytes, filename

    with zipfile.ZipFile(io.BytesIO(file_bytes)) as archive:
        file_entries = [name for name in archive.namelist() if not name.endswith("/")]
        if len(file_entries) != 1:
            raise ValueError(f"Archiv für {filename} enthält nicht genau eine Datei.")
        inner_name = file_entries[0]
        return archive.read(inner_name), Path(inner_name).name


def load_table(uploaded_file, default_path: Path | None, kind: str) -> tuple[pd.DataFrame, str]:
    if uploaded_file is not None:
        filename = uploaded_file.name
        file_bytes = uploaded_file.getvalue()
    elif default_path is not None and default_path.exists():
        filename = default_path.name
        file_bytes = default_path.read_bytes()
    else:
        raise FileNotFoundError(f"Keine Datei für {kind} gefunden.")

    file_bytes, detected_name = extract_if_zipped(file_bytes, filename)
    filename = detected_name or filename
    suffix = Path(filename).suffix.lower()
    if suffix == ".csv":
        csv_text = decode_csv_bytes(file_bytes)
        dataframe = pd.read_csv(
            io.StringIO(csv_text),
            sep=";",
            dtype=str,
            keep_default_na=False,
            engine="python",
        )
    elif suffix in {".xlsx", ".xls"}:
        dataframe = pd.read_excel(io.BytesIO(file_bytes), dtype=str)
        dataframe = dataframe.fillna("")
    else:
        raise ValueError(f"Dateiformat für {kind} nicht unterstützt: {suffix}")

    dataframe.columns = [str(column).strip() for column in dataframe.columns]
    return dataframe, filename


def validate_columns(dataframe: pd.DataFrame, required_columns: set[str], label: str) -> None:
    missing = sorted(required_columns - set(dataframe.columns))
    if missing:
        raise ValueError(f"In {label} fehlen diese Spalten: {', '.join(missing)}")


def prepare_orders_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    df = dataframe.copy()
    for column in [
        "Firma",
        "Kundennummer",
        "Herold-Nummer",
        "Zugewiesen an",
        "Produkt",
        "Abo",
        "Kündigungsdatum",
    ]:
        df[column] = df[column].astype(str).str.strip()

    df["is_active_abo_customer"] = (df["Abo"] == "Ja") & (df["Kündigungsdatum"] == "")
    df["Beginn_Datum"] = pd.to_datetime(df["Beginn"], dayfirst=True, errors="coerce")
    df["customer_name_normalized"] = df["Firma"].map(normalize_text)
    df["kundennummer_norm"] = df["Kundennummer"].astype(str).str.strip()
    df["herold_nummer_norm"] = df["Herold-Nummer"].astype(str).str.strip()
    df["match_key"] = df["kundennummer_norm"]
    df.loc[df["match_key"] == "", "match_key"] = "HN:" + df["herold_nummer_norm"]
    df.loc[df["match_key"].isin({"", "HN:"}), "match_key"] = "NAME:" + df["customer_name_normalized"]
    return df


def prepare_contacts_dataframe(dataframe: pd.DataFrame, current_sellers: list[str]) -> pd.DataFrame:
    df = dataframe.copy()
    for column in [
        "Datum/Uhrzeit",
        "Mitarbeiter",
        "Kontaktbericht für",
        "Kontakt",
        "Kontaktart",
        "Kundennummer",
        "Herold-Nummer (Firma)",
    ]:
        if column in df.columns:
            df[column] = df[column].astype(str).str.strip()

    df["contact_datetime"] = pd.to_datetime(df["Datum/Uhrzeit"], dayfirst=True, errors="coerce")
    df["kontakt_name_normalized"] = df["Kontakt"].map(normalize_text)
    df["kundennummer_norm"] = df["Kundennummer"].astype(str).str.strip()
    df["herold_nummer_norm"] = df["Herold-Nummer (Firma)"].astype(str).str.strip()
    df["contact_bucket"] = "Sonstiges"
    df.loc[df["Kontaktart"].isin(SUCCESSFUL_CONTACT_TYPES), "contact_bucket"] = "Erfolgreich"
    df.loc[df["Kontaktart"].isin(ATTEMPT_CONTACT_TYPES), "contact_bucket"] = "Versuch"
    df["is_current_seller"] = df["Mitarbeiter"].isin(current_sellers)
    df = df[df["is_current_seller"]].copy()
    return df


def deduplicate_customer_base(active_orders: pd.DataFrame) -> pd.DataFrame:
    working = active_orders.copy()

    grouped = (
        working.sort_values(["Firma", "Zugewiesen an", "Produkt"])
        .groupby("match_key", dropna=False)
        .agg(
            Firma=("Firma", "first"),
            Kundennummer=("Kundennummer", "first"),
            Herold_Nummer=("Herold-Nummer", "first"),
            Zugewiesen_An=("Zugewiesen an", lambda values: ", ".join(sorted({value for value in values if value}))),
            Abo_Produkte=("Produkt", lambda values: ", ".join(sorted({value for value in values if value}))),
            Anzahl_Abo_Auftraege=("Produkt", "size"),
            customer_name_normalized=("customer_name_normalized", "first"),
            kundennummer_norm=("kundennummer_norm", "first"),
            herold_nummer_norm=("herold_nummer_norm", "first"),
        )
        .reset_index()
    )
    return grouped


def build_first_order_dates(orders: pd.DataFrame) -> pd.DataFrame:
    first_orders = (
        orders.dropna(subset=["Beginn_Datum"])
        .groupby("match_key", dropna=False)
        .agg(
            Erstauftrag=("Beginn_Datum", "min"),
            Erste_Firma=("Firma", "first"),
        )
        .reset_index()
    )
    return first_orders


def build_match_maps(customer_base: pd.DataFrame) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    by_customer_number: dict[str, str] = {}
    by_herold_number: dict[str, str] = {}
    by_name: dict[str, str] = {}

    for _, row in customer_base.iterrows():
        customer_label = row["Firma"]
        if row["kundennummer_norm"]:
            by_customer_number[row["kundennummer_norm"]] = customer_label
        if row["herold_nummer_norm"]:
            by_herold_number[row["herold_nummer_norm"]] = customer_label
        if row["customer_name_normalized"] and row["customer_name_normalized"] not in by_name:
            by_name[row["customer_name_normalized"]] = customer_label

    return by_customer_number, by_herold_number, by_name


def match_contacts_to_customers(contacts: pd.DataFrame, customer_base: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    by_customer_number, by_herold_number, by_name = build_match_maps(customer_base)
    matched = contacts.copy()
    matched["matched_customer"] = ""
    matched["match_source"] = ""

    customer_number_mask = matched["kundennummer_norm"].isin(by_customer_number)
    matched.loc[customer_number_mask, "matched_customer"] = matched.loc[customer_number_mask, "kundennummer_norm"].map(by_customer_number)
    matched.loc[customer_number_mask, "match_source"] = "Kundennummer"

    herold_mask = (matched["matched_customer"] == "") & matched["herold_nummer_norm"].isin(by_herold_number)
    matched.loc[herold_mask, "matched_customer"] = matched.loc[herold_mask, "herold_nummer_norm"].map(by_herold_number)
    matched.loc[herold_mask, "match_source"] = "Herold-Nummer"

    name_mask = (matched["matched_customer"] == "") & matched["kontakt_name_normalized"].isin(by_name)
    matched.loc[name_mask, "matched_customer"] = matched.loc[name_mask, "kontakt_name_normalized"].map(by_name)
    matched.loc[name_mask, "match_source"] = "Firmenname"

    matched_contacts = matched[matched["matched_customer"] != ""].copy()
    unmatched_contacts = matched[matched["matched_customer"] == ""].copy()
    return matched_contacts, unmatched_contacts


def average_gap_days(datetimes: pd.Series) -> float | None:
    valid = datetimes.dropna().sort_values().drop_duplicates()
    if len(valid) < 2:
        return None
    day_deltas = valid.diff().dropna().dt.total_seconds().div(86400)
    if day_deltas.empty:
        return None
    return round(float(day_deltas.mean()), 1)


def describe_frequency(total_contacts: int, avg_gap: float | None) -> str:
    if total_contacts == 0:
        return "Noch kein Kontakt"
    if avg_gap is None or pd.isna(avg_gap):
        return f"{total_contacts} Kontakt"
    rounded_gap = int(round(avg_gap))
    if total_contacts == 1:
        return f"1 Kontakt"
    return f"{total_contacts} Kontakte, Ø {rounded_gap} Tage Abstand"


def last_value_for_group(values: pd.Series, datetimes: pd.Series) -> str:
    valid = pd.DataFrame({"value": values, "datetime": datetimes}).dropna(subset=["datetime"])
    if valid.empty:
        return ""
    return str(valid.sort_values("datetime").iloc[-1]["value"])


def months_between(start: pd.Timestamp | pd.NaT, end: pd.Timestamp) -> int | None:
    if pd.isna(start):
        return None
    months = (end.year - start.year) * 12 + (end.month - start.month)
    if end.day < start.day:
        months -= 1
    return max(months, 0)


def build_customer_contact_summary(
    customer_base: pd.DataFrame,
    matched_contacts: pd.DataFrame,
    first_order_dates: pd.DataFrame,
    no_contact_grace_months: int,
) -> pd.DataFrame:
    now = pd.Timestamp.now().normalize()

    contact_summary = (
        matched_contacts.groupby("matched_customer", dropna=False)
        .agg(
            Letzter_Kontakt=("contact_datetime", "max"),
            Anzahl_Kontakte=("contact_datetime", "size"),
            Anzahl_Erfolgreich=("contact_bucket", lambda values: int((pd.Series(values) == "Erfolgreich").sum())),
            Anzahl_Versuche=("contact_bucket", lambda values: int((pd.Series(values) == "Versuch").sum())),
            Letzter_erfolgreicher_Kontakt=("contact_datetime", lambda values: values[matched_contacts.loc[values.index, "contact_bucket"] == "Erfolgreich"].max()),
            Letzter_Kontaktversuch=("contact_datetime", lambda values: values[matched_contacts.loc[values.index, "contact_bucket"] == "Versuch"].max()),
            Letzter_Kontakt_durch=(
                "Mitarbeiter",
                lambda values: last_value_for_group(values, matched_contacts.loc[values.index, "contact_datetime"]),
            ),
            Letzte_Kontaktart=(
                "Kontaktart",
                lambda values: last_value_for_group(values, matched_contacts.loc[values.index, "contact_datetime"]),
            ),
            Avg_Abstand_Tage=("contact_datetime", average_gap_days),
            Avg_Abstand_Erfolgreich=("contact_datetime", lambda values: average_gap_days(values[matched_contacts.loc[values.index, "contact_bucket"] == "Erfolgreich"])),
        )
        .reset_index()
        .rename(columns={"matched_customer": "Firma"})
    )

    summary = customer_base.merge(contact_summary, on="Firma", how="left")
    summary = summary.merge(first_order_dates[["match_key", "Erstauftrag"]], on="match_key", how="left")

    for column in ["Anzahl_Kontakte", "Anzahl_Erfolgreich", "Anzahl_Versuche"]:
        summary[column] = summary[column].fillna(0).astype(int)

    summary["Tage_seit_letztem_Kontakt"] = (now - summary["Letzter_Kontakt"]).dt.days
    summary["Monate_seit_Erstauftrag"] = summary["Erstauftrag"].map(lambda value: months_between(value, now))
    summary["Kein_Kontakt_in_Schonfrist"] = (
        (summary["Anzahl_Kontakte"] == 0)
        & summary["Monate_seit_Erstauftrag"].notna()
        & (summary["Monate_seit_Erstauftrag"] < no_contact_grace_months)
    )
    summary["Kein_Kontakt_faellig"] = (
        (summary["Anzahl_Kontakte"] == 0)
        & (
            summary["Monate_seit_Erstauftrag"].isna()
            | (summary["Monate_seit_Erstauftrag"] >= no_contact_grace_months)
        )
    )
    summary["Kontaktfrequenz"] = [
        describe_frequency(total_contacts, avg_gap)
        for total_contacts, avg_gap in zip(summary["Anzahl_Kontakte"], summary["Avg_Abstand_Tage"])
    ]
    summary["Erfolgsfrequenz"] = [
        describe_frequency(total_contacts, avg_gap)
        for total_contacts, avg_gap in zip(summary["Anzahl_Erfolgreich"], summary["Avg_Abstand_Erfolgreich"])
    ]
    summary["Kontaktstatus"] = pd.cut(
        summary["Tage_seit_letztem_Kontakt"],
        bins=[-float("inf"), 30, 90, 180, float("inf")],
        labels=["<= 30 Tage", "31-90 Tage", "91-180 Tage", "> 180 Tage"],
    )
    summary["Kontaktstatus"] = summary["Kontaktstatus"].astype("object").fillna("Noch kein Kontakt")
    summary.loc[summary["Kein_Kontakt_in_Schonfrist"], "Kontaktstatus"] = "Kein Kontakt, innerhalb Schonfrist"
    summary.loc[summary["Kein_Kontakt_faellig"], "Kontaktstatus"] = "Kein Kontakt, fällig"
    return summary


def build_seller_summary(customer_summary: pd.DataFrame, matched_contacts: pd.DataFrame) -> pd.DataFrame:
    contacts_per_seller = (
        matched_contacts.groupby("Mitarbeiter", dropna=False)
        .agg(
            Kontakte_gesamt=("contact_datetime", "size"),
            Kontakte_erfolgreich=("contact_bucket", lambda values: int((pd.Series(values) == "Erfolgreich").sum())),
            Kontaktversuche=("contact_bucket", lambda values: int((pd.Series(values) == "Versuch").sum())),
            Letzter_Kontakt=("contact_datetime", "max"),
            Kunden_mit_Kontakt=("matched_customer", lambda values: values.nunique()),
            Durchschnittlicher_Abstand_Tage=("contact_datetime", average_gap_days),
        )
        .reset_index()
        .rename(columns={"Mitarbeiter": "Verkäufer"})
    )

    assigned_seller_base = customer_summary.copy()
    assigned_seller_base["Verkäufer"] = assigned_seller_base["Zugewiesen_An"].fillna("")
    assigned_seller_base["Verkäufer"] = assigned_seller_base["Verkäufer"].map(lambda value: value.split(",")[0].strip() if value else "")

    customer_counts = (
        assigned_seller_base.groupby("Verkäufer", dropna=False)
        .agg(
            Aktive_Abo_Kunden=("Firma", "nunique"),
            Kunden_ohne_Kontakt_faellig=("Kein_Kontakt_faellig", lambda values: int(pd.Series(values).fillna(False).sum())),
            Kunden_ohne_Kontakt_in_Schonfrist=(
                "Kein_Kontakt_in_Schonfrist",
                lambda values: int(pd.Series(values).fillna(False).sum()),
            ),
        )
        .reset_index()
    )

    seller_summary = customer_counts.merge(contacts_per_seller, on="Verkäufer", how="left")
    for column in [
        "Kontakte_gesamt",
        "Kontakte_erfolgreich",
        "Kontaktversuche",
        "Kunden_mit_Kontakt",
        "Kunden_ohne_Kontakt_faellig",
        "Kunden_ohne_Kontakt_in_Schonfrist",
    ]:
        seller_summary[column] = seller_summary[column].fillna(0).astype(int)
    seller_summary["Durchschnittlicher_Abstand_Tage"] = seller_summary["Durchschnittlicher_Abstand_Tage"].round(1)
    seller_summary["Kontaktfrequenz"] = [
        describe_frequency(total_contacts, avg_gap)
        for total_contacts, avg_gap in zip(
            seller_summary["Kontakte_gesamt"], seller_summary["Durchschnittlicher_Abstand_Tage"]
        )
    ]
    return seller_summary.sort_values(["Aktive_Abo_Kunden", "Kontakte_gesamt"], ascending=[False, False])


def format_datetime(series: pd.Series) -> pd.Series:
    return series.map(lambda value: value.strftime("%d.%m.%Y %H:%M") if pd.notna(value) else "")


def format_date(series: pd.Series) -> pd.Series:
    return series.map(lambda value: value.strftime("%d.%m.%Y") if pd.notna(value) else "")


def render_data_overview(
    customer_summary: pd.DataFrame,
    matched_contacts: pd.DataFrame,
    unmatched_contacts: pd.DataFrame,
    sellers: list[str],
    sources: DataSources,
    no_contact_grace_months: int,
    contacts_range: tuple[pd.Timestamp | pd.NaT, pd.Timestamp | pd.NaT],
) -> None:
    active_customers = int(customer_summary["Firma"].nunique())
    customers_with_contact = int((customer_summary["Anzahl_Kontakte"] > 0).sum())
    customers_without_contact_due = int(summary_bool_count(customer_summary["Kein_Kontakt_faellig"]))
    customers_without_contact_grace = int(summary_bool_count(customer_summary["Kein_Kontakt_in_Schonfrist"]))
    matched_contact_count = int(len(matched_contacts))

    metric_columns = st.columns(5)
    metric_columns[0].metric("Aktive Abo-Kunden", active_customers)
    metric_columns[1].metric("Kunden mit Kontakt", customers_with_contact)
    metric_columns[2].metric("Kein Kontakt, fällig", customers_without_contact_due)
    metric_columns[3].metric("Gematchte Kontaktberichte", matched_contact_count)
    metric_columns[4].metric("Kein Kontakt, Schonfrist", customers_without_contact_grace)

    with st.expander("Verwendete Datenquellen und Matching", expanded=False):
        st.write(f"Aufträge: `{sources.orders_name}`")
        st.write(f"Kontaktberichte: `{sources.contacts_name}`")
        range_start, range_end = contacts_range
        if pd.notna(range_start) and pd.notna(range_end):
            st.write(
                "Ausgewerteter Zeitraum der Kontaktberichte: "
                f"`{range_start.strftime('%d.%m.%Y %H:%M')}` bis `{range_end.strftime('%d.%m.%Y %H:%M')}`"
            )
        st.write(f"Schonfrist für `kein Kontakt`: {no_contact_grace_months} Monate nach Erstauftrag.")
        st.write(
            "Matching-Reihenfolge: zuerst `Kundennummer`, dann `Herold-Nummer`, danach normalisierter Firmenname `Kontakt` zu `Firma`."
        )
        st.write(
            f"Nicht gematchte Kontaktberichte aktueller Verkäufer: {len(unmatched_contacts)}. "
            "Diese Kontakte betreffen typischerweise Nicht-Kunden, Neukunden oder Datensätze ohne eindeutigen Kundenbezug."
        )


def summary_bool_count(series: pd.Series) -> int:
    return int(pd.Series(series).fillna(False).sum())


def main() -> None:
    st.set_page_config(page_title="Kontaktberichte bei Abo-Kunden", layout="wide")
    st.title("Kontaktberichte bei Abo-Kunden")
    st.caption(
        "Zeigt für aktive Abo-Kunden, welcher Verkäufer zuletzt Kontakt hatte, wie oft Kontakt stattfand "
        "und in welchem durchschnittlichen Abstand."
    )

    sellers_file = resolve_sellers_file()
    current_sellers = read_sellers(sellers_file)
    if not current_sellers:
        expected_names = ", ".join(f"`{name}`" for name in SELLERS_FILE_CANDIDATES)
        st.error(
            "Die Datei mit aktiven Verkäufern wurde nicht gefunden oder ist leer. "
            f"Erwartet wird eine TXT-Datei wie {expected_names}."
        )
        st.stop()

    default_orders = find_default_file("Aufträge")
    default_contacts = find_default_file("Kontaktberichte")

    with st.sidebar:
        st.header("Daten laden")
        st.write("Uploads überschreiben die Testdateien aus dem Projektordner.")
        orders_upload = st.file_uploader(
            "Auftragsliste hochladen",
            type=["csv", "xlsx", "xls"],
            accept_multiple_files=False,
        )
        contacts_upload = st.file_uploader(
            "Kontaktberichte hochladen",
            type=["csv", "xlsx", "xls"],
            accept_multiple_files=False,
        )
        st.write(f"Aktive Verkäuferdatei: `{sellers_file.name}`")
        st.write(f"Gefundene Verkäufer: {len(current_sellers)}")
        no_contact_grace_months = st.slider(
            "Schonfrist ohne Kontakt nach Erstauftrag (Monate)",
            min_value=0,
            max_value=24,
            value=6,
            step=1,
        )
        seller_filter = st.multiselect("Verkäufer filtern", options=current_sellers, default=current_sellers)
        search_term = st.text_input("Kunde suchen", placeholder="Firma oder Kundennummer")

    try:
        orders_raw, orders_name = load_table(orders_upload, default_orders, "Auftragsliste")
        contacts_raw, contacts_name = load_table(contacts_upload, default_contacts, "Kontaktberichte")
        validate_columns(orders_raw, ORDERS_REQUIRED_COLUMNS, "Auftragsliste")
        validate_columns(contacts_raw, CONTACTS_REQUIRED_COLUMNS, "Kontaktberichte")
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    orders = prepare_orders_dataframe(orders_raw)
    contacts = prepare_contacts_dataframe(contacts_raw, current_sellers)

    active_orders = orders[orders["is_active_abo_customer"]].copy()
    customer_base = deduplicate_customer_base(active_orders)
    first_order_dates = build_first_order_dates(orders)
    matched_contacts, unmatched_contacts = match_contacts_to_customers(contacts, customer_base)
    customer_summary = build_customer_contact_summary(
        customer_base,
        matched_contacts,
        first_order_dates,
        no_contact_grace_months,
    )
    seller_summary = build_seller_summary(customer_summary, matched_contacts)

    available_contact_statuses = [
        "<= 30 Tage",
        "31-90 Tage",
        "91-180 Tage",
        "> 180 Tage",
        "Kein Kontakt, fällig",
        "Kein Kontakt, innerhalb Schonfrist",
    ]
    contact_status_filter = st.sidebar.multiselect(
        "Kontaktstatus filtern",
        options=available_contact_statuses,
        default=available_contact_statuses,
    )

    if seller_filter:
        customer_summary = customer_summary[
            customer_summary["Zugewiesen_An"].map(
                lambda value: any(seller in {part.strip() for part in str(value).split(",")} for seller in seller_filter)
            )
        ].copy()
        seller_summary = seller_summary[seller_summary["Verkäufer"].isin(seller_filter)].copy()
        matched_contacts = matched_contacts[matched_contacts["Mitarbeiter"].isin(seller_filter)].copy()

    if contact_status_filter:
        customer_summary = customer_summary[customer_summary["Kontaktstatus"].isin(contact_status_filter)].copy()

    if search_term:
        needle = normalize_text(search_term)
        customer_summary = customer_summary[
            customer_summary["Firma"].map(normalize_text).str.contains(needle, na=False)
            | customer_summary["Kundennummer"].astype(str).str.contains(search_term, na=False)
        ].copy()

    sources = DataSources(orders_name=orders_name, contacts_name=contacts_name)
    contacts_range = contact_export_range(contacts)
    range_start, range_end = contacts_range
    if pd.notna(range_start) and pd.notna(range_end):
        st.info(
            "Die Kontakt-Auswertung basiert nur auf dem geladenen Exportzeitraum: "
            f"{range_start.strftime('%d.%m.%Y %H:%M')} bis {range_end.strftime('%d.%m.%Y %H:%M')}."
        )
    render_data_overview(
        customer_summary,
        matched_contacts,
        unmatched_contacts,
        current_sellers,
        sources,
        no_contact_grace_months,
        contacts_range,
    )

    seller_tab, customer_tab, contact_tab = st.tabs(
        ["Verkäufer-Übersicht", "Kunden-Details", "Kontaktberichte"]
    )

    with seller_tab:
        display = seller_summary.copy()
        display["Letzter_Kontakt"] = format_datetime(display["Letzter_Kontakt"])
        st.dataframe(
            display[
                [
                    "Verkäufer",
                    "Aktive_Abo_Kunden",
                    "Kunden_mit_Kontakt",
                    "Kunden_ohne_Kontakt_faellig",
                    "Kunden_ohne_Kontakt_in_Schonfrist",
                    "Kontakte_gesamt",
                    "Kontakte_erfolgreich",
                    "Kontaktversuche",
                    "Kontaktfrequenz",
                    "Letzter_Kontakt",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    with customer_tab:
        display = customer_summary.copy()
        display["Erstauftrag"] = format_date(display["Erstauftrag"])
        display["Letzter_Kontakt"] = format_datetime(display["Letzter_Kontakt"])
        display["Letzter_erfolgreicher_Kontakt"] = format_datetime(display["Letzter_erfolgreicher_Kontakt"])
        display["Letzter_Kontaktversuch"] = format_datetime(display["Letzter_Kontaktversuch"])
        st.dataframe(
            display[
                [
                    "Firma",
                    "Kundennummer",
                    "Zugewiesen_An",
                    "Erstauftrag",
                    "Monate_seit_Erstauftrag",
                    "Abo_Produkte",
                    "Anzahl_Abo_Auftraege",
                    "Kontaktstatus",
                    "Letzter_Kontakt",
                    "Letzter_Kontakt_durch",
                    "Letzte_Kontaktart",
                    "Letzter_erfolgreicher_Kontakt",
                    "Letzter_Kontaktversuch",
                    "Anzahl_Kontakte",
                    "Anzahl_Erfolgreich",
                    "Anzahl_Versuche",
                    "Kontaktfrequenz",
                    "Erfolgsfrequenz",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    with contact_tab:
        display = matched_contacts.copy()
        display["Datum/Uhrzeit"] = format_datetime(display["contact_datetime"])
        st.dataframe(
            display[
                [
                    "Datum/Uhrzeit",
                    "Mitarbeiter",
                    "matched_customer",
                    "Kontaktart",
                    "contact_bucket",
                    "Kontakt",
                    "Kundennummer",
                    "Herold-Nummer (Firma)",
                    "match_source",
                ]
            ].rename(
                columns={
                    "matched_customer": "Gematchter Kunde",
                    "contact_bucket": "Kontakt-Typ",
                    "match_source": "Match über",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )


if __name__ == "__main__":
    main()
