"""
games_config.py - إعدادات الألعاب وتطبيقات الاختبار
يحتوي على بيانات الحزم، مفاتيح التطوير (Dev Keys)، وقوالب الأحداث.
"""

GAMES_DATA = [
    {
        "name": "Color Sort Woody Puzzle",
        "package": "games.burny.color.sort.woody.puzzle",
        "dev_key": "CkyYk6ThNSr9MPEvmCNZRN",
        "level_events": [{"display": "Level completed", "template": "level_completed"}],
        "cat": "puzzle"
    },
    {
        "name": "Wooden Word Puzzle",
        "package": "games.burny.wooden.word.puzzle",
        "dev_key": "CkyYk6ThNSr9MPEvmCNZRN",
        "level_events": [{"display": "Level completed", "template": "level_completed_{}"}],
        "cat": "puzzle"
    },
    {
        "name": "Associations Word Puzzle",
        "package": "games.burny.associations.word.puzzle",
        "dev_key": "CkyYk6ThNSr9MPEvmCNZRN",
        "level_events": [{"display": "Level completed", "template": "level_completed_{}"}],
        "cat": "puzzle"
    },
    {
        "name": "Coin Master",
        "package": "com.moonactive.coinmaster",
        "dev_key": "H3KjoCRVTiVgA5mWSAHtCe",
        "level_events": [{"display": "Village complete", "template": "village_{}_complete"}],
        "cat": "other"
    },
    {
        "name": "Domino Dreams",
        "package": "com.screenshake.dominodreams",
        "dev_key": "Hn5qYjVAaRNJYDcwF4LaWF",
        "level_events": [{"display": "Area completed", "template": "af_area_{}_completed"}],
        "cat": "other"
    },
    {
        "name": "Solitaire Grand Harvest",
        "package": "net.supertreat.solitaire",
        "dev_key": "X8aGgdWELNb5L96fCy44oL",
        "level_events": [{"display": "Level up", "template": "level_up_{}"}],
        "cat": "other"
    },
    {
        "name": "Cuisine Master",
        "package": "com.FortuneMine.CuisineMaster",
        "dev_key": "im6mgZbZJsHKGVowkkxkGm",
        "level_events": [{"display": "Level completed", "template": "level{}_completed"}],
        "cat": "other"
    },
    {
        "name": "Match Factory",
        "package": "net.peakgames.match",
        "dev_key": "F9M4SkdtH8WHcAt86ESrF3",
        "level_events": [{"display": "Level completed", "template": "level_{}_completed"}],
        "cat": "puzzle"
    },
    {
        "name": "Toon Blast",
        "package": "net.peakgames.toonblast",
        "dev_key": "F9M4SkdtH8WHcAt86ESrF3",
        "level_events": [{"display": "Level completed", "template": "level_{}_completed"}],
        "cat": "other"
    },
    {
        "name": "Toy Blast",
        "package": "net.peakgames.amy",
        "dev_key": "F9M4SkdtH8WHcAt86ESrF3",
        "level_events": [{"display": "Level completed", "template": "level_{}_completed"}],
        "cat": "other"
    },
    {
        "name": "Box Jam",
        "package": "com.playoneer.boxjam",
        "dev_key": "cSJjCEwX2cCtJrNH92xM7n",
        "level_events": [{"display": "Level completed", "template": "level_completed"}],
        "cat": "puzzle"
    },
    {
        "name": "Gardenscapes",
        "package": "com.playrix.gardenscapes",
        "dev_key": "ixMH7WQi4SqLAmPWuyAtkH",
        "level_events": [{"display": "Level achieved", "template": "AchievedLevel{}"}],
        "cat": "other"
    },
    {
        "name": "Dice Dreams",
        "package": "com.superplaystudios.dicedreams",
        "dev_key": "Hn5qYjVAaRNJYDcwF4LaWF",
        "level_events": [{"display": "Kingdom restored", "template": "af_kingdom_{}_restored"}],
        "cat": "other"
    },
    {
        "name": "Taylor's Secret",
        "package": "com.mergegames.taylorsecret",
        "dev_key": "qbKVyawAiwNX3b4D3Frijm",
        "level_events": [{"display": "Level achieved", "template": "level_{}"}],
        "cat": "merge"
    },
    {
        "name": "Big Farm Homestead",
        "package": "com.newmoonproduction.bigfarmhomestead",
        "dev_key": "FtaT5WH9rMJjJkMd4LfBCT",
        "level_events": [{"display": "Level achieved", "template": "af_level{}_achieved"}],
        "cat": "farm"
    },
    {
        "name": "Sunshine Island",
        "package": "com.newmoonproduction.sunshineisland",
        "dev_key": "FtaT5WH9rMJjJkMd4LfBCT",
        "level_events": [{"display": "Level achieved", "template": "af_level{}_achieved"}],
        "cat": "farm"
    },
    {
        "name": "Idle Bank Tycoon",
        "package": "com.luckyskeletonstudios.idlebanktycoon",
        "dev_key": "DwzgMQDkEVMy3ntcgz7utV",
        "level_events": [{"display": "Bank started", "template": "af_bank{}_started"}],
        "cat": "other"
    },
    {
        "name": "FarmVille 3",
        "package": "com.zynga.farmville3",
        "dev_key": "KVJuxDJVXQRPoqUVdyWJtj",
        "level_events": [{"display": "Player level", "template": "Player_Level{}"}],
        "cat": "farm"
    },
    {
        "name": "Township",
        "package": "com.playrix.township",
        "dev_key": "ixMH7WQi4SqLAmPWuyAtkH",
        "level_events": [{"display": "Achieved level", "template": "achievedlevel{}"}],
        "cat": "farm"
    },
    {
        "name": "Merge Match",
        "package": "com.futureplay.mergematch",
        "dev_key": "nr8SibwpFjcKGBQNpDdttd",
        "level_events": [{"display": "Incent Level Up", "template": "Incent_Player_Level_Up_{}"}],
        "cat": "merge"
    },
    {
        "name": "FarmVille 2",
        "package": "com.zynga.FarmVille2CountryEscape",
        "dev_key": "438VCPmX2ZLYvsDPfGLZXb",
        "level_events": [{"display": "Level complete", "template": "Level{}Complete"}],
        "cat": "farm"
    },
    {
        "name": "Merge Studio",
        "package": "com.paxiegames.mergestudio",
        "dev_key": "mFaiXvTWy8votzp3UpsSfS",
        "level_events": [{"display": "Level completed", "template": "level_completed_{}"}],
        "cat": "merge"
    },
    {
        "name": "Goods Master 3D",
        "package": "com.goods.master3d.triple.puzzle",
        "dev_key": "oUYXWsx5SVDbJwtSCCymUD",
        "level_events": [{"display": "Level achieved", "template": "af_Level_{}"}],
        "cat": "puzzle"
    },
    {
        "name": "Tiles Mosaic",
        "package": "com.tripledot.tiles.mosaic",
        "dev_key": "j7J8eEwxvuQbDDFo3agC9X",
        "level_events": [{"display": "Game complete", "template": "GameComplete_{}"}],
        "cat": "puzzle"
    },
    {
        "name": "Travel Town",
        "package": "io.randomco.travel",
        "dev_key": "wizhvjciCuaDbAaR8KpZLn",
        "level_events": [{"display": "Level achieved", "template": "reachedLevel{}"}],
        "cat": "other"
    },
    {
        "name": "Disney Solitaire Dreams",
        "package": "com.superplaystudios.disneysolitairedreams",
        "dev_key": "Hn5qYjVAaRNJYDcwF4LaWF",
        "level_events": [{"display": "Area completed", "template": "af_area_{}_completed"}],
        "cat": "other"
    },
    {
        "name": "Match Masters",
        "package": "com.funtomic.matchmasters",
        "dev_key": "EJcrH2pxmBbsWKHJHdrs3c",
        "level_events": [{"display": "Trophies reached", "template": "trophies_{}"}],
        "cat": "other"
    },
    {
        "name": "Coin Master Board",
        "package": "com.moonactive.cmboard",
        "dev_key": "H3KjoCRVTiVgA5mWSAHtCe",
        "level_events": [{"display": "Village complete", "template": "village_{}_complete"}],
        "cat": "other"
    },
    {
        "name": "Empires & Puzzles",
        "package": "com.smallgiantgames.empires",
        "dev_key": "wStj8eCCuE84shUTkh7ZGc",
        "level_events": [{"display": "XP Level reached", "template": "xp_level_{}"}],
        "cat": "other"
    },
    {
        "name": "Merge Dragons",
        "package": "com.gramgames.mergedragons",
        "dev_key": "fyKVEgAYzuD6jBZocaq3yh",
        "level_events": [{"display": "Dragon Power reached", "template": "event_{}_dragon_power"}],
        "cat": "merge"
    },
    {
        "name": "Yarn Loop",
        "package": "com.combo.yarnflow",
        "dev_key": "TGm97uKTJF7qFCvREggWtf",
        "level_events": [{"display": "Level completed", "template": "level_complete_{}"}],
        "cat": "other"
    }
]
