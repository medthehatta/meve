from concurrent.futures import ThreadPoolExecutor
import pickle
import os.path
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
