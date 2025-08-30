# policy.py — shared policy helpers

def flag_from_row(row: dict) -> str:
    """
    Derive a flag string from a row if needed.
    Placeholder: returns 🟢 GREEN by default.
    Adjust logic if your rankers expect something else.
    """
    return row.get("Flag", "🟢 GREEN")