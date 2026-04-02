TEAM_COLORS = {
    # 2023-2025 teams with official colors
    "Red Bull Racing": "#3671C6",
    "Ferrari": "#E8002D",
    "Mercedes": "#27F4D2",
    "McLaren": "#FF8000",
    "Aston Martin": "#229971",
    "Alpine": "#FF87BC",
    "Williams": "#64C4FF",
    "AlphaTauri": "#6692FF",
    "RB": "#6692FF",
    "Haas F1 Team": "#B6BABD",
    "Alfa Romeo": "#C92D4B",
    "Stake F1 Team Kick Sauber": "#52E252",
    "Sauber": "#52E252",
    "Visa Cash App RB": "#6692FF",
}

FALLBACK_COLORS = [
    "#E6C229", "#F17105", "#D11149", "#6610F2", "#1A8FE3",
    "#44BBA4", "#E94F37", "#393E41", "#F5A623", "#7B2D8B",
]


def get_driver_colors(drivers_df):
    """Return a dict mapping driver full_name -> hex color based on team."""
    color_map = {}
    fallback_idx = 0
    for _, row in drivers_df.iterrows():
        team = row.get("TEAM_NAME", "")
        name = row.get("FULL_NAME", "")
        if team in TEAM_COLORS:
            color_map[name] = TEAM_COLORS[team]
        else:
            color_map[name] = FALLBACK_COLORS[fallback_idx % len(FALLBACK_COLORS)]
            fallback_idx += 1
    return color_map


def altair_color_scale(color_map):
    """Return an Altair scale with explicit domain/range from a color_map."""
    import altair as alt
    return alt.Scale(
        domain=list(color_map.keys()),
        range=list(color_map.values()),
    )
