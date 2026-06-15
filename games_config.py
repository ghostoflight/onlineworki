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
        "name": "FarmVille 2 Country Escape",
        "package": "com.zynga.FarmVille2CountryEscape",
        "dev_key": "438VCPmX2ZLYvsDPfGLZXb",
        "level_events": [{"display": "Level complete", "template": "Level{}Complete"}],
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
    },
    {
        "name": "Domino Dreams (iOS)",
        "package": "id6444043291",
        "dev_key": "Hn5qYjVAaRNJYDcwF4LaWF",
        "level_events": [{"display": "Area completed", "template": "af_area_{}_completed"}],
        "cat": "ios"
    },
    {
        "name": "Disney Solitaire (iOS)",
        "package": "id6475757306",
        "dev_key": "Hn5qYjVAaRNJYDcwF4LaWF",
        "level_events": [{"display": "Area completed", "template": "af_area_{}_completed"}],
        "cat": "ios"
    },
    {
        "name": "Coin Master (iOS)",
        "package": "id406889139",
        "dev_key": "H3KjoCRVTiVgA5mWSAHtCe",
        "level_events": [{"display": "Village complete", "template": "village_{}_complete"}],
        "cat": "ios"
    },
    {
        "name": "Board Adventure (iOS)",
        "package": "id6745761596",
        "dev_key": "H3KjoCRVTiVgA5mWSAHtCe",
        "level_events": [{"display": "Village complete", "template": "village_{}_complete"}],
        "cat": "ios"
    },
    {
        "name": "Dice Dreams (iOS)",
        "package": "id1484468651",
        "dev_key": "Hn5qYjVAaRNJYDcwF4LaWF",
        "level_events": [{"display": "Kingdom restored", "template": "af_kingdom_{}_restored"}],
        "cat": "ios"
    },
    {
        "name": "Toon Blast (iOS)",
        "package": "id1176027022",
        "dev_key": "F9M4SkdtH8WHcAt86ESrF3",
        "level_events": [{"display": "Level completed", "template": "level_{}_completed"}],
        "cat": "ios"
    },
    {
        "name": "Match Factory (iOS)",
        "package": "id6444094229",
        "dev_key": "F9M4SkdtH8WHcAt86ESrF3",
        "level_events": [{"display": "Level completed", "template": "level_{}_completed"}],
        "cat": "ios"
    },
    {
        "name": "Gardenscapes (iOS)",
        "package": "id1105855019",
        "dev_key": "ixMH7WQi4SqLAmPWuyAtkH",
        "level_events": [{"display": "Achieved level", "template": "AchievedLevel{}"}],
        "cat": "ios"
    },
    {
        "name": "Homescapes (iOS)",
        "package": "id1195621598",
        "dev_key": "ixMH7WQi4SqLAmPWuyAtkH",
        "level_events": [{"display": "Achieved level", "template": "AchievedLevel{}"}],
        "cat": "ios"
    },
    {
        "name": "Fishdom (iOS)",
        "package": "id664575829",
        "dev_key": "ixMH7WQi4SqLAmPWuyAtkH",
        "level_events": [{"display": "Achieved level", "template": "acheivedlevel{}"}],
        "cat": "ios"
    },
    {
        "name": "Township (iOS)",
        "package": "id63868907",
        "dev_key": "nr8SibwpFjcKGBQNpDdttd",
        "level_events": [{"display": "Achieved level", "template": "achievedlevel{}"}],
        "cat": "ios"
    },
    {
        "name": "Travel Town (iOS)",
        "package": "id1521236603",
        "dev_key": "wizhvjciCuaDbAaR8KpZLn",
        "level_events": [{"display": "Reached level", "template": "reachedLevel{}"}],
        "cat": "ios"
    },
    {
        "name": "Merge Studio (iOS)",
        "package": "id1615964753",
        "dev_key": "mFaiXvTWy8votzp3UpsSfS",
        "level_events": [{"display": "Level completed", "template": "level_completed_{}"}],
        "cat": "ios"
    },
    {
        "name": "Taylor's Secret (iOS)",
        "package": "id1659615328",
        "dev_key": "qbKVyawAiwNX3b4D3Frijm",
        "level_events": [{"display": "Level achieved", "template": "level_{}"}],
        "cat": "ios"
    },
    {
        "name": "Yarn Loop! (iOS)",
        "package": "id6755183085",
        "dev_key": "TGm97uKTJF7qFCvREggWtf",
        "level_events": [{"display": "Level complete", "template": "level_complete_{}"}],
        "cat": "ios"
    },
    {
        "name": "Klondike (iOS)",
        "package": "id1127240206",
        "dev_key": "oTiS8cJmQRTDXbixob6wQL",
        "level_events": [{"display": "Level achieved", "template": "level {}"}],
        "cat": "ios"
    },
    {
        "name": "screw guru (iOS)",
        "package": "id6737529244",
        "dev_key": "nYwfftoacbopmuszWBPGnd",
        "level_events": [{"display": "Level achieved", "template": "af_level_achieved{}"}],
        "cat": "ios"
    },
    {
        "name": "Solitaire Smash (iOS)",
        "package": "id6446482475",
        "dev_key": "PuaMuZXWm4zYzfDQNwW9Nf",
        "level_events": [{"display": "Deposit and win", "template": "deposit_0_and_win_{}"}],
        "cat": "ios"
    },
    {
        "name": "Solitaire Cash (iOS)",
        "package": "id1446254576",
        "dev_key": "HwQpkdp5VP6pSvK3Cd486g",
        "level_events": [{"display": "Reached level", "template": "ReachedLevel{}"}],
        "cat": "ios"
    },
    {
        "name": "haunted merge (iOS)",
        "package": "id6752369665",
        "dev_key": "6mUaoEXrUfgJUnRCue6ZeM",
        "level_events": [{"display": "HR level", "template": "hr_level_{}"}],
        "cat": "ios"
    },
    {
        "name": "Merge Gardens (iOS)",
        "package": "id1464675244",
        "dev_key": "nr8SibwpFjcKGBQNpDdttd",
        "level_events": [{"display": "Incent player level up", "template": "Incent_Player_Level_Up_{}"}],
        "cat": "ios"
    }
]
