'''To make formatting quicker'''
def format_currency(value):
    return f"${value:,.2f}"

def summarize_metrics(users, sessions, avg_duration):
    summary = (
        f"Over the selected period, we had {users:,} unique users generating {sessions:,} sessions. "
        f"Average session duration was {avg_duration:.2f} minutes."
    )
    return summary