"""The tracking-sheet update, against a stand-in for the Smartsheet client."""

from datetime import date
from types import SimpleNamespace as NS

import pytest
from upload_capsule.smartsheet import SHEET, SmartsheetError, mark_registered


class FakeSheets:
    def __init__(self, sheets, sheet):
        self.sheets, self.sheet, self.updates = sheets, sheet, []

    def list_sheets(self, include_all):
        return NS(data=self.sheets)

    def get_sheet(self, sheet_id):
        return self.sheet

    def update_rows(self, sheet_id, rows):
        self.updates.append((sheet_id, rows))


def _client(rows, columns=("Subject", "CCF Registered", "Affine Registration Date"), name=SHEET):
    sheet = NS(
        columns=[NS(title=t, id=i) for i, t in enumerate(columns, start=100)],
        rows=[NS(id=rid, cells=[NS(display_value=v) for v in values]) for rid, values in rows],
    )
    return NS(Sheets=FakeSheets([NS(name="other", id=1), NS(name=name, id=7)], sheet))


def test_the_subjects_row_is_marked_registered():
    client = _client([(11, ["823506"]), (12, ["823507", "x"])])
    mark_registered("823507", "token", date(2026, 10, 2), client=client)
    ((sheet_id, (row,)),) = client.Sheets.updates
    assert (sheet_id, row.id) == (7, 12)
    assert [(c.column_id, c.value) for c in row.cells] == [(101, True), (102, "10/02/2026")]


@pytest.mark.parametrize(
    "client, message",
    [
        (_client([(11, ["823507"])], name="renamed"), "No sheet"),
        (_client([(11, ["823507"])], columns=("Subject",)), "no column"),
        (_client([(11, ["999999"])]), "No row for subject 823507"),
    ],
)
def test_what_cannot_be_found_is_named(client, message):
    with pytest.raises(SmartsheetError, match=message):
        mark_registered("823507", "token", client=client)
    assert client.Sheets.updates == []
