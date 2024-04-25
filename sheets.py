from concurrent.futures import ThreadPoolExecutor
import pickle
import os.path
from functools import reduce
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import gspread
from gspread.client import Client

from util import prefix


relpath = prefix(__file__)


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]


def login(credential_file_path):
    creds = None
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            tok_info = pickle.load(token)
            if all(scp in tok_info["scopes"] for scp in SCOPES):
                creds = tok_info["creds"]
    if not creds or not creds.valid:
        if os.path.exists("token.pickle"):
            os.remove("token.pickle")
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credential_file_path, SCOPES
            )
            creds = flow.run_console()
        with open("token.pickle", "wb") as token:
            pickle.dump(
                {"scopes": SCOPES, "creds": creds}, token,
            )

    return Client(creds)


def service_login(service_account_file):
    return gspread.service_account(filename=service_account_file)


def entries(
    client,
    url,
    sheet="Sheet1",
    range_=None,
    indirect=None,
    force_header=None,
    skip_first_line=False,
):
    book = client.open_by_url(url)
    sheet_ = book.worksheet(sheet)

    if range_:
        data = sheet_.get(range_)
        return [dict(zip(data[0], entry)) for entry in data[1:]]
    elif indirect:
        sheet_range = sheet_.get(indirect)[0][0]
        return entries(client, url, sheet, rng=sheet_range)
    elif force_header:
        rows = [dict(zip(force_header, row)) for row in sheet_.get_values()]
        if skip_first_line:
            return rows[1:]
        else:
            return rows
    else:
        return sheet_.get_all_records()


def gsheet(id_):
    return f"https://docs.google.com/spreadsheets/d/{id_}/edit#gid=0"


def google_sheet_reader(url, tab="Sheet1", **kwargs):

    def _google_sheet_reader():
        # FIXME: Ugh, configuring the sheets client is painful.
        # Just setting it as a constant from the file for now
        client = service_login(relpath("service-account.json"))
        return entries(client, url, sheet=tab, **kwargs)

    return _google_sheet_reader


def get_row_range(rng):
    return rng[0]


def to_row_range(seq):
    return [list(seq)]


def map_row_range(func, seq):
    return [func(x) for x in list(seq[0])]


def threadmap_row_range(func, seq, max_workers=4):
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        return list(exe.map(func, list(seq[0])))


def get_col_range(rng):
    return [x[0] for x in rng]


def to_col_range(seq):
    return [[x] for x in seq]


def map_col_range(func, seq):
    return [[func(x[0])] for x in seq]


def threadmap_col_range(func, seq, max_workers=4):
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        return list(exe.map(lambda x: [func(x[0])], seq))


def records_to_columns(records):
    if not records:
        return {}

    fields = list(records[0].keys())
    result = {
        field: [record.get(field) for record in records]
        for field in fields
    }

    len_first = len(result[fields[0]])
    if any(len(col) != len_first for col in result.values()):
        raise ValueError(f"Ragged columns: {result}")

    return result


def columns_to_records(columns):
    if not columns:
        return []

    header = list(columns.keys())
    len_first = len(columns[header[0]])
    if any(len(col) != len_first for col in columns.values()):
        raise ValueError(f"Ragged columns: {columns}")

    return [
        dict(zip(header, record))
        for record in zip(*[columns[field] for field in header])
    ]


def read_records(
    sheet,
    header_row=1,
    numericize=True,
    always_float=False,
    skip_rendered_empty=True,
    skip_when_field_empty=None,
):
    skip_when_field_empty = skip_when_field_empty or []

    def _maybe_numeric(value):
        if not numericize:
            return value

        cleaned = reduce(
            lambda acc, x: acc.replace(x, ""),
            ["$", ","],
            value,
        )

        if "." in cleaned or always_float:
            try:
                return float(cleaned)
            except (ValueError, TypeError):
                return cleaned
        else:
            try:
                return int(cleaned)
            except (ValueError, TypeError):
                return cleaned

    # We are 0-indexed but the sheet is 1-indexed
    header_row0 = header_row - 1

    fields = sheet.get_values()[header_row0]
    field_dupe_counts = {}
    uniq_fields = []
    for field in fields:
        if field in field_dupe_counts:
            field_dupe_counts[field] += 1
            uniq_fields.append(f"{field}##{field_dupe_counts[field]}")
        else:
            uniq_fields.append(field)
            field_dupe_counts[field] = 1
    records = sheet.get_values()[header_row0 + 1:]
    result = [
        dict(zip(uniq_fields, [_maybe_numeric(value) for value in record]))
        for record in records
    ]

    filtered = result[:]

    if skip_rendered_empty:
        filtered = [res for res in filtered if any(r != "" for r in res.values())]

    if skip_when_field_empty:
        filtered = [
            res for res in filtered
            if all(res.get(field) != "" for field in skip_when_field_empty)
        ]

    return filtered


def insert_records(
    sheet,
    records,
    header_row=1,
    field_translation=None,
):
    field_translation = field_translation or {}
    fields = sheet.get_values(f"A{header_row}:{header_row}")[0]
    matrix = [
        [
            record.get(field_translation.get(field, field), "")
            for field in fields
        ]
        for record in records
    ]
    sheet.update(
        range_name=f"A{header_row+1}",
        values=matrix,
    )
    return matrix


def append_records(
    sheet,
    records,
    header_row=1,
    field_translation=None,
):
    field_translation = field_translation or {}
    fields = sheet.get_values(f"A{header_row}:{header_row}")[0]
    matrix = [
        [
            record.get(field_translation.get(field, field), "")
            for field in fields
        ]
        for record in records
    ]
    last_row = len(sheet.get_values())
    sheet.update(
        range_name=f"A{last_row+1}",
        values=matrix,
    )
    return matrix


def populate_from_index(
    in_sheet,
    out_sheet=None,
    index_col="A",
    header_row=1,
    top_left: str = None,
    header_translate={},
    data_for: callable = None,
):
    top_left = top_left or f"{index_col}{header_row}"

    data_for = data_for or {}

    index_values = in_sheet.get_values(f"{index_col}{header_row+1}:{index_col}")
    rows = [
        data_for(value[0]) for value in index_values
    ]
    header_values = in_sheet.get_values(f"{index_col}{header_row}:{header_row}")

    matrix = (
        header_values +
        [
            [
                row.get(header_translate.get(hval, hval), None)
                for hval in header_values[0]
            ]
            for row in rows
        ]
    )

    sheet.update(f"{index_col}{header_row}", matrix)
