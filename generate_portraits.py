"""Generate SVG portrait placeholders for each driver with their number and team color."""
import os

OUTDIR = os.path.join(os.path.dirname(__file__), "static", "img", "portraits")
os.makedirs(OUTDIR, exist_ok=True)

TEAM_COLORS = {
    "Red Bull": "#3671C6",
    "Ferrari": "#E8002D",
    "McLaren": "#FF8000",
    "Mercedes": "#27F4D2",
    "Aston Martin": "#229971",
    "Alpine": "#FF87BC",
    "Racing Bulls": "#6692FF",
    "Audi": "#ff0000",
    "Williams": "#64C4FF",
    "Haas": "#B6BABD",
    "Cadillac": "#c0a44d",
}

DRIVERS = [
    ("Max Verstappen", "Red Bull", 3),
    ("Isack Hadjar", "Red Bull", 6),
    ("Lewis Hamilton", "Ferrari", 44),
    ("Charles Leclerc", "Ferrari", 16),
    ("Lando Norris", "McLaren", 1),
    ("Oscar Piastri", "McLaren", 81),
    ("George Russell", "Mercedes", 63),
    ("Kimi Antonelli", "Mercedes", 12),
    ("Fernando Alonso", "Aston Martin", 14),
    ("Lance Stroll", "Aston Martin", 18),
    ("Pierre Gasly", "Alpine", 10),
    ("Franco Colapinto", "Alpine", 43),
    ("Liam Lawson", "Racing Bulls", 30),
    ("Arvid Lindblad", "Racing Bulls", 41),
    ("Nico Hulkenberg", "Audi", 27),
    ("Gabriel Bortoleto", "Audi", 5),
    ("Alexander Albon", "Williams", 23),
    ("Carlos Sainz", "Williams", 55),
    ("Oliver Bearman", "Haas", 87),
    ("Esteban Ocon", "Haas", 31),
    ("Sergio Perez", "Cadillac", 11),
    ("Valtteri Bottas", "Cadillac", 77),
]


def darken(hex_color, factor=0.4):
    hex_color = hex_color.lstrip("#")
    r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
    return f"#{int(r*factor):02x}{int(g*factor):02x}{int(b*factor):02x}"


def make_svg(name, team, number):
    color = TEAM_COLORS.get(team, "#666")
    dark = darken(color, 0.3)
    initials = "".join(w[0] for w in name.split()[:2]).upper()
    hex_c = color.lstrip("#")
    lum = 0.299 * int(hex_c[0:2], 16) + 0.587 * int(hex_c[2:4], 16) + 0.114 * int(hex_c[4:6], 16)
    text_color = "#000" if lum > 160 else "#fff"

    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 200" width="200" height="200">
  <defs>
    <linearGradient id="bg" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" style="stop-color:{color};stop-opacity:1"/>
      <stop offset="100%" style="stop-color:{dark};stop-opacity:1"/>
    </linearGradient>
  </defs>
  <rect width="200" height="200" rx="20" fill="url(#bg)"/>
  <text x="100" y="90" text-anchor="middle" dominant-baseline="middle"
        font-family="Inter,Arial,sans-serif" font-size="52" font-weight="800"
        fill="{text_color}" opacity="0.9">{initials}</text>
  <text x="100" y="145" text-anchor="middle" dominant-baseline="middle"
        font-family="Inter,Arial,sans-serif" font-size="48" font-weight="900"
        fill="{text_color}" opacity="0.5">{number}</text>
</svg>'''


for name, team, number in DRIVERS:
    slug = name.lower().replace(" ", "-").replace(".", "")
    with open(os.path.join(OUTDIR, f"{slug}.svg"), "w") as f:
        f.write(make_svg(name, team, number))
    print(f"  {slug}.svg")

print("Done!")
