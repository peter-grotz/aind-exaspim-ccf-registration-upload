"""Mark the subject as registered in the ExM tracking sheet, through the Smartsheet API."""

from __future__ import annotations

from datetime import date, datetime, timezone

SHEET = "ExM Dataset Summary"
REGISTERED = "CCF Registered"
REGISTRATION_DATE = "Affine Registration Date"


class SmartsheetError(LookupError):
    """Raised when the sheet, the subject's row or a column cannot be found."""


def mark_registered(subject_id: str, token: str, today: date | None = None, client=None) -> None:
    """Tick ``CCF Registered`` and set ``Affine Registration Date`` on the subject's row.

    The row is the first one with a cell whose displayed value is the subject id.

    Parameters
    ----------
    subject_id : str
        Subject whose row is updated.
    token : str
        Smartsheet API token.
    today : date | None, optional
        Date to record; defaults to today in UTC.
    client : smartsheet.Smartsheet | None, optional
        API client; built from ``token`` when omitted.

    Raises
    ------
    SmartsheetError
        If the sheet, the row or either column is missing.
    """
    import smartsheet

    if client is None:
        client = smartsheet.Smartsheet(token)
        client.errors_as_exceptions(True)

    sheet_id = next(
        (s.id for s in client.Sheets.list_sheets(include_all=True).data if s.name == SHEET), None
    )
    if sheet_id is None:
        raise SmartsheetError(f"No sheet named {SHEET!r}")
    sheet = client.Sheets.get_sheet(sheet_id)

    columns = {column.title: column.id for column in sheet.columns}
    missing = [title for title in (REGISTERED, REGISTRATION_DATE) if title not in columns]
    if missing:
        raise SmartsheetError(f"{SHEET!r} has no column(s) {missing}")
    row_id = next(
        (r.id for r in sheet.rows if any(c.display_value == subject_id for c in r.cells)), None
    )
    if row_id is None:
        raise SmartsheetError(f"No row for subject {subject_id} in {SHEET!r}")

    when = today or datetime.now(timezone.utc).date()
    row = smartsheet.models.Row(
        {
            "id": row_id,
            "cells": [
                {"column_id": columns[REGISTERED], "value": True, "strict": False},
                {
                    "column_id": columns[REGISTRATION_DATE],
                    "value": f"{when:%m/%d/%Y}",
                    "strict": False,
                },
            ],
        }
    )
    client.Sheets.update_rows(sheet_id, [row])
