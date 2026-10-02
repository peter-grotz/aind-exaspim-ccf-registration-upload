"""Mark the subject as registered in the ExM tracking sheet."""

from __future__ import annotations

from datetime import date, datetime, timezone

SHEET = "ExM Dataset Summary"


def mark_registered(subject_id: str, token: str, today: date | None = None) -> None:
    """Tick ``CCF Registered`` and set ``Affine Registration Date`` for the subject's row.

    Parameters
    ----------
    subject_id : str
        Subject whose row is updated.
    token : str
        Smartsheet API token.
    today : date | None, optional
        Date to record; defaults to today in UTC.
    """
    from aind_exaspim_dataset_utils.smartsheet_util import SmartSheetClient

    client = SmartSheetClient(token, SHEET)
    columns = {column.title: column.id for column in client.sheet.columns}
    row = client.client.models.Row()
    row.id = client.find_row_id(subject_id)
    row.cells.append({"column_id": columns.get("CCF Registered"), "value": True, "strict": False})
    row.cells.append(
        {
            "column_id": columns.get("Affine Registration Date"),
            "value": f"{(today or datetime.now(timezone.utc).date()):%m/%d/%Y}",
            "strict": False,
        }
    )
    client.client.Sheets.update_rows(client.sheet_id, [row])
