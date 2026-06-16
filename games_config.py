"""
games_config.py - إعدادات الألعاب وتطبيقات الاختبار
يحتوي على بيانات الحزم، مفاتيح التطوير (Dev Keys)، وقوالب الأحداث.
بنية متسلسلة: OS → فئة → تطبيق
"""

GAMES_DATA = [
    # ═════════════════════════════════════════════════════════
    # ألعاب Android
    # ═════════════════════════════════════════════════════════
    {
        "name": "Color Sort Woody Puzzle",
        "package": "games.burny.color.sort.woody.puzzle",
        "dev_key": "CkyYk6ThNSr9MPEvmCNZRN",
        "level_events": [{"display": "Level completed", "template": "level_completed"}],
        "os": "android",
        "cat": "puzzle"
    },
    {
        "name": "Wooden Word Puzzle",
        "package": "games.burny.wooden.word.puzzle",
        "dev_key": "CkyYk6ThNSr9MPEvmCNZRN",
        "level_events": [{"display": "Level completed", "template": "level_completed_{}"}],
        "os": "android",
        "cat": "puzzle"
    },
    {
        "name": "Associations Word Puzzle",
        "package": "games.burny.associations.word.puzzle",
        "dev_key": "CkyYk6ThNSr9MPEvmCNZRN",
        "level_events": [{"display": "Level completed", "template": "level_completed_{}"}],
        "os": "android",
        "cat": "puzzle"
    },
    {
        "name": "Coin Master",
        "package": "com.moonactive.coinmaster",
        "dev_key": "H3KjoCRVTiVgA5mWSAHtCe",
        "level_events": [{"display": "Village complete", "template": "village_{}_complete"}],
        "os": "android",
        "cat": "other"
    },
    {
        "name": "Domino Dreams",
        "package": "com.screenshake.dominodreams",
        "dev_key": "Hn5qYjVAaRNJYDcwF4LaWF",
        "level_events": [{"display": "Area completed", "template": "af_area_{}_completed"}],
        "os": "android",
        "cat": "other"
    },
    {
        "name": "Solitaire Grand Harvest",
        "package": "net.supertreat.solitaire",
        "dev_key": "X8aGgdWELNb5L96fCy44oL",
        "level_events": [{"display": "Level up", "template": "level_up_{}"}],
        "os": "android",
        "cat": "other"
    },
    {
        "name": "Cuisine Master",
        "package": "com.FortuneMine.CuisineMaster",
        "dev_key": "im6mgZbZJsHKGVowkkxkGm",
        "level_events": [{"display": "Level completed", "template": "level{}_completed"}],
        "os": "android",
        "cat": "other"
    },
    {
        "name": "Match Factory",
        "package": "net.peakgames.match",
        "dev_key": "F9M4SkdtH8WHcAt86ESrF3",
        "level_events": [{"display": "Level completed", "template": "level_{}_completed"}],
        "os": "android",
        "cat": "puzzle"
    },
    {
        "name": "Toon Blast",
        "package": "net.peakgames.toonblast",
        "dev_key": "F9M4SkdtH8WHcAt86ESrF3",
        "level_events": [{"display": "Level completed", "template": "level_{}_completed"}],
        "os": "android",
        "cat": "other"
    },
    {
        "name": "Toy Blast",
        "package": "net.peakgames.amy",
        "dev_key": "F9M4SkdtH8WHcAt86ESrF3",
        "level_events": [{"display": "Level completed", "template": "level_{}_completed"}],
        "os": "android",
        "cat": "other"
    },
    {
        "name": "Box Jam",
        "package": "com.playoneer.boxjam",
        "dev_key": "cSJjCEwX2cCtJrNH92xM7n",
        "level_events": [{"display": "Level completed", "template": "level_completed"}],
        "os": "android",
        "cat": "puzzle"
    },
    {
        "name": "Gardenscapes",
        "package": "com.playrix.gardenscapes",
        "dev_key": "ixMH7WQi4SqLAmPWuyAtkH",
        "level_events": [{"display": "Level achieved", "template": "AchievedLevel{}"}],
        "os": "android",
        "cat": "other"
    },
    {
        "name": "Dice Dreams",
        "package": "com.superplaystudios.dicedreams",
        "dev_key": "Hn5qYjVAaRNJYDcwF4LaWF",
        "level_events": [{"display": "Kingdom restored", "template": "af_kingdom_{}_restored"}],
        "os": "android",
        "cat": "other"
    },
    {
        "name": "Taylor's Secret",
        "package": "com.mergegames.taylorsecret",
        "dev_key": "qbKVyawAiwNX3b4D3Frijm",
        "level_events": [{"display": "Level achieved", "template": "level_{}"}],
        "os": "android",
        "cat": "merge"
    },
    {
        "name": "Big Farm Homestead",
        "package": "com.newmoonproduction.bigfarmhomestead",
        "dev_key": "FtaT5WH9rMJjJkMd4LfBCT",
        "level_events": [{"display": "Level achieved", "template": "af_level{}_achieved"}],
        "os": "android",
        "cat": "farm"
    },
    {
        "name": "Sunshine Island",
        "package": "com.newmoonproduction.sunshineisland",
        "dev_key": "FtaT5WH9rMJjJkMd4LfBCT",
        "level_events": [{"display": "Level achieved", "template": "af_level{}_achieved"}],
        "os": "android",
        "cat": "farm"
    },
    {
        "name": "Idle Bank Tycoon",
        "package": "com.luckyskeletonstudios.idlebanktycoon",
        "dev_key": "DwzgMQDkEVMy3ntcgz7utV",
        "level_events": [{"display": "Bank started", "template": "af_bank{}_started"}],
        "os": "android",
        "cat": "other"
    },
    {
        "name": "FarmVille 2 Country Escape",
        "package": "com.zynga.FarmVille2CountryEscape",
        "dev_key": "438VCPmX2ZLYvsDPfGLZXb",
        "level_events": [{"display": "Level complete", "template": "Level{}Complete"}],
        "os": "android",
        "cat": "farm"
    },
    {
        "name": "Township",
        "package": "com.playrix.township",
        "dev_key": "ixMH7WQi4SqLAmPWuyAtkH",
        "level_events": [{"display": "Achieved level", "template": "achievedlevel{}"}],
        "os": "android",
        "cat": "farm"
    },
    {
        "name": "Merge Match",
        "package": "com.futureplay.mergematch",
        "dev_key": "nr8SibwpFjcKGBQNpDdttd",
        "level_events": [{"display": "Incent Level Up", "template": "Incent_Player_Level_Up_{}"}],
        "os": "android",
        "cat": "merge"
    },
    {
        "name": "FarmVille 2",
        "package": "com.zynga.FarmVille2CountryEscape",
        "dev_key": "438VCPmX2ZLYvsDPfGLZXb",
        "level_events": [{"display": "Level complete", "template": "Level{}Complete"}],
        "os": "android",
        "cat": "farm"
    },
    {
        "name": "Merge Studio",
        "package": "com.paxiegames.mergestudio",
        "dev_key": "mFaiXvTWy8votzp3UpsSfS",
        "level_events": [{"display": "Level completed", "template": "level_completed_{}"}],
        "os": "android",
        "cat": "merge"
    },
    {
        "name": "Goods Master 3D",
        "package": "com.goods.master3d.triple.puzzle",
        "dev_key": "oUYXWsx5SVDbJwtSCCymUD",
        "level_events": [{"display": "Level achieved", "template": "af_Level_{}"}],
        "os": "android",
        "cat": "puzzle"
    },
    {
        "name": "Tiles Mosaic",
        "package": "com.tripledot.tiles.mosaic",
        "dev_key": "j7J8eEwxvuQbDDFo3agC9X",
        "level_events": [{"display": "Game complete", "template": "GameComplete_{}"}],
        "os": "android",
        "cat": "puzzle"
    },
    {
        "name": "Travel Town",
        "package": "io.randomco.travel",
        "dev_key": "wizhvjciCuaDbAaR8KpZLn",
        "level_events": [{"display": "Level achieved", "template": "reachedLevel{}"}],
        "os": "android",
        "cat": "other"
    },
    {
        "name": "Disney Solitaire Dreams",
        "package": "com.superplaystudios.disneysolitairedreams",
        "dev_key": "Hn5qYjVAaRNJYDcwF4LaWF",
        "level_events": [{"display": "Area completed", "template": "af_area_{}_completed"}],
        "os": "android",
        "cat": "other"
    },
    {
        "name": "Match Masters",
        "package": "com.funtomic.matchmasters",
        "dev_key": "EJcrH2pxmBbsWKHJHdrs3c",
        "level_events": [{"display": "Trophies reached", "template": "trophies_{}"}],
        "os": "android",
        "cat": "other"
    },
    {
        "name": "Coin Master Board",
        "package": "com.moonactive.cmboard",
        "dev_key": "H3KjoCRVTiVgA5mWSAHtCe",
        "level_events": [{"display": "Village complete", "template": "village_{}_complete"}],
        "os": "android",
        "cat": "other"
    },
    {
        "name": "Empires & Puzzles",
        "package": "com.smallgiantgames.empires",
        "dev_key": "wStj8eCCuE84shUTkh7ZGc",
        "level_events": [{"display": "XP Level reached", "template": "xp_level_{}"}],
        "os": "android",
        "cat": "other"
    },
    {
        "name": "Merge Dragons",
        "package": "com.gramgames.mergedragons",
        "dev_key": "fyKVEgAYzuD6jBZocaq3yh",
        "level_events": [{"display": "Dragon Power reached", "template": "event_{}_dragon_power"}],
        "os": "android",
        "cat": "merge"
    },
    {
        "name": "Yarn Loop",
        "package": "com.combo.yarnflow",
        "dev_key": "TGm97uKTJF7qFCvREggWtf",
        "level_events": [{"display": "Level completed", "template": "level_complete_{}"}],
        "os": "android",
        "cat": "other"
    },

    # ═════════════════════════════════════════════════════════
    # ألعاب iOS
    # ═════════════════════════════════════════════════════════
    {
        "name": "Domino Dreams (iOS)",
        "package": "id6444043291",
        "dev_key": "Hn5qYjVAaRNJYDcwF4LaWF",
        "level_events": [{"display": "Area completed", "template": "af_area_{}_completed"}],
        "os": "ios",
        "cat": "other"
    },
    {
        "name": "Disney Solitaire (iOS)",
        "package": "id6475757306",
        "dev_key": "Hn5qYjVAaRNJYDcwF4LaWF",
        "level_events": [{"display": "Area completed", "template": "af_area_{}_completed"}],
        "os": "ios",
        "cat": "other"
    },
    {
        "name": "Coin Master (iOS)",
        "package": "id406889139",
        "dev_key": "H3KjoCRVTiVgA5mWSAHtCe",
        "level_events": [{"display": "Village complete", "template": "village_{}_complete"}],
        "os": "ios",
        "cat": "other"
    },
    {
        "name": "Board Adventure (iOS)",
        "package": "id6745761596",
        "dev_key": "H3KjoCRVTiVgA5mWSAHtCe",
        "level_events": [{"display": "Village complete", "template": "village_{}_complete"}],
        "os": "ios",
        "cat": "other"
    },
    {
        "name": "Dice Dreams (iOS)",
        "package": "id1484468651",
        "dev_key": "Hn5qYjVAaRNJYDcwF4LaWF",
        "level_events": [{"display": "Kingdom restored", "template": "af_kingdom_{}_restored"}],
        "os": "ios",
        "cat": "other"
    },
    {
        "name": "Toon Blast (iOS)",
        "package": "id1176027022",
        "dev_key": "F9M4SkdtH8WHcAt86ESrF3",
        "level_events": [{"display": "Level completed", "template": "level_{}_completed"}],
        "os": "ios",
        "cat": "other"
    },
    {
        "name": "Match Factory (iOS)",
        "package": "id6444094229",
        "dev_key": "F9M4SkdtH8WHcAt86ESrF3",
        "level_events": [{"display": "Level completed", "template": "level_{}_completed"}],
        "os": "ios",
        "cat": "puzzle"
    },
    {
        "name": "Gardenscapes (iOS)",
        "package": "id1105855019",
        "dev_key": "ixMH7WQi4SqLAmPWuyAtkH",
        "level_events": [{"display": "Achieved level", "template": "AchievedLevel{}"}],
        "os": "ios",
        "cat": "other"
    },
    {
        "name": "Homescapes (iOS)",
        "package": "id1195621598",
        "dev_key": "ixMH7WQi4SqLAmPWuyAtkH",
        "level_events": [{"display": "Achieved level", "template": "AchievedLevel{}"}],
        "os": "ios",
        "cat": "other"
    },
    {
        "name": "Fishdom (iOS)",
        "package": "id664575829",
        "dev_key": "ixMH7WQi4SqLAmPWuyAtkH",
        "level_events": [{"display": "Achieved level", "template": "acheivedlevel{}"}],
        "os": "ios",
        "cat": "other"
    },
    {
        "name": "Township (iOS)",
        "package": "id63868907",
        "dev_key": "nr8SibwpFjcKGBQNpDdttd",
        "level_events": [{"display": "Achieved level", "template": "achievedlevel{}"}],
        "os": "ios",
        "cat": "farm"
    },
    {
        "name": "Travel Town (iOS)",
        "package": "id1521236603",
        "dev_key": "wizhvjciCuaDbAaR8KpZLn",
        "level_events": [{"display": "Reached level", "template": "reachedLevel{}"}],
        "os": "ios",
        "cat": "other"
    },
    {
        "name": "Merge Studio (iOS)",
        "package": "id1615964753",
        "dev_key": "mFaiXvTWy8votzp3UpsSfS",
        "level_events": [{"display": "Level completed", "template": "level_completed_{}"}],
        "os": "ios",
        "cat": "merge"
    },
    {
        "name": "Taylor's Secret (iOS)",
        "package": "id1659615328",
        "dev_key": "qbKVyawAiwNX3b4D3Frijm",
        "level_events": [{"display": "Level achieved", "template": "level_{}"}],
        "os": "ios",
        "cat": "merge"
    },
    {
        "name": "Yarn Loop! (iOS)",
        "package": "id6755183085",
        "dev_key": "TGm97uKTJF7qFCvREggWtf",
        "level_events": [{"display": "Level complete", "template": "level_complete_{}"}],
        "os": "ios",
        "cat": "other"
    },
    {
        "name": "Klondike (iOS)",
        "package": "id1127240206",
        "dev_key": "oTiS8cJmQRTDXbixob6wQL",
        "level_events": [{"display": "Level achieved", "template": "level {}"}],
        "os": "ios",
        "cat": "other"
    },
    {
        "name": "screw guru (iOS)",
        "package": "id6737529244",
        "dev_key": "nYwfftoacbopmuszWBPGnd",
        "level_events": [{"display": "Level achieved", "template": "af_level_achieved{}"}],
        "os": "ios",
        "cat": "puzzle"
    },
    {
        "name": "Solitaire Smash (iOS)",
        "package": "id6446482475",
        "dev_key": "PuaMuZXWm4zYzfDQNwW9Nf",
        "level_events": [{"display": "Deposit and win", "template": "deposit_0_and_win_{}"}],
        "os": "ios",
        "cat": "other"
    },
    {
        "name": "Solitaire Cash (iOS)",
        "package": "id1446254576",
        "dev_key": "HwQpkdp5VP6pSvK3Cd486g",
        "level_events": [{"display": "Reached level", "template": "ReachedLevel{}"}],
        "os": "ios",
        "cat": "other"
    },
    {
        "name": "haunted merge (iOS)",
        "package": "id6752369665",
        "dev_key": "6mUaoEXrUfgJUnRCue6ZeM",
        "level_events": [{"display": "HR level", "template": "hr_level_{}"}],
        "os": "ios",
        "cat": "merge"
    },
    {
        "name": "Merge Gardens (iOS)",
        "package": "id1464675244",
        "dev_key": "nr8SibwpFjcKGBQNpDdttd",
        "level_events": [{"display": "Incent player level up", "template": "Incent_Player_Level_Up_{}"}],
        "os": "ios",
        "cat": "merge"
    }
]


# ── دوال وصول مساعدة (يستخدمها التدفّق المتسلسل) ──────────────────────────────
def list_os():
    """أنظمة التشغيل المتاحة (بالترتيب)."""
    seen = []
    for g in GAMES_DATA:
        o = g.get("os", "android")
        if o not in seen:
            seen.append(o)
    return seen


def list_categories(os_name):
    """فئات تطبيقات نظام تشغيل معيّن."""
    cats = []
    for g in GAMES_DATA:
        if g.get("os") == os_name and g.get("cat") and g["cat"] not in cats:
            cats.append(g["cat"])
    return cats


def list_games(os_name, cat):
    """(index, game) للتطبيقات ضمن نظام/فئة محدّدين — index هو موضعه في GAMES_DATA."""
    return [(i, g) for i, g in enumerate(GAMES_DATA)
            if g.get("os") == os_name and g.get("cat") == cat]
