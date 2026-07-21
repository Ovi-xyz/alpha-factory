# instruments_raw.py — [ARCHIVED, v1.11.2]
# Original Grand Design v1.2 instrument constants (643 instruments, flat
# 4-market structure, no Layer 2). Superseded by config/instruments.yaml
# (now hand-maintained directly, v1.5, 699 instruments, hierarchical).
# Relocated from src/config/instruments_raw.py — its only consumer,
# migrate_instruments.py, is archived and guarded in this same directory.
# Kept for historical reference only. See scripts/archive/README.md.

US_STOCKS_BY_SECTOR = {

    # ── 1. TECHNOLOGY ──────────────────────────────────────────────────────
    "Technology": [
        "AAPL",  # Apple Inc.
        "MSFT",  # Microsoft Corp.
        "NVDA",  # NVIDIA Corp.
        "GOOGL", # Alphabet Inc. (Class A)
        "GOOG",  # Alphabet Inc. (Class C)
        "META",  # Meta Platforms
        "AVGO",  # Broadcom Inc.
        "ORCL",  # Oracle Corp.
        "CRM",   # Salesforce Inc.
        "AMD",   # Advanced Micro Devices
        "INTC",  # Intel Corp.
        "QCOM",  # Qualcomm Inc.
        "TXN",   # Texas Instruments
        "NOW",   # ServiceNow
        "INTU",  # Intuit Inc.
        "IBM",   # IBM Corp.
        "AMAT",  # Applied Materials
        "LRCX",  # Lam Research
        "KLAC",  # KLA Corp.
        "MU",    # Micron Technology
        "ADI",   # Analog Devices
        "MCHP",  # Microchip Technology
        "CDNS",  # Cadence Design Systems
        "SNPS",  # Synopsys Inc.
        "FTNT",  # Fortinet Inc.
        "PANW",  # Palo Alto Networks
        "CRWD",  # CrowdStrike Holdings
        "SNOW",  # Snowflake Inc.
        "PLTR",  # Palantir Technologies
        "DDOG",  # Datadog Inc.
        "NET",   # Cloudflare Inc.
        "ZS",    # Zscaler Inc.
        "OKTA",  # Okta Inc.
        "WDAY",  # Workday Inc.
        "ANSS",  # ANSYS Inc.
        "PTC",   # PTC Inc.
        "ANET",  # Arista Networks
        "HPQ",   # HP Inc.
        "HPE",   # Hewlett Packard Enterprise
        "DELL",  # Dell Technologies
        "NTAP",  # NetApp Inc.
        "STX",   # Seagate Technology
        "WDC",   # Western Digital
        "KEYS",  # Keysight Technologies
        "JNPR",  # Juniper Networks
        "CIEN",  # Ciena Corp.
        "SWKS",  # Skyworks Solutions
        "MRVL",  # Marvell Technology
        "ON",    # ON Semiconductor
        "GFS",   # GlobalFoundries
        "MPWR",  # Monolithic Power Systems
        "ENTG",  # Entegris Inc.
        "MKSI",  # MKS Instruments
        "COHR",  # Coherent Corp.
        "SMCI",  # Super Micro Computer
        "ARM",   # ARM Holdings
        "ACLS",  # Axcelis Technologies
        "ICHR",  # Ichor Holdings
        "ONTO",  # Onto Innovation
        "FORM",  # FormFactor Inc.
        "AMBA",  # Ambarella Inc.
        "ALGM",  # Allegro MicroSystems
        "DIOD",  # Diodes Inc.
        "WOLF",  # Wolfspeed Inc.
    ],

    # ── 2. CONSUMER DISCRETIONARY ──────────────────────────────────────────
    "Consumer Discretionary": [
        "AMZN",  # Amazon.com
        "TSLA",  # Tesla Inc.
        "HD",    # Home Depot
        "MCD",   # McDonald's Corp.
        "NKE",   # Nike Inc.
        "LOW",   # Lowe's Companies
        "SBUX",  # Starbucks Corp.
        "TJX",   # TJX Companies
        "BKNG",  # Booking Holdings
        "CMG",   # Chipotle Mexican Grill
        "GM",    # General Motors
        "F",     # Ford Motor Co.
        "RIVN",  # Rivian Automotive
        "LCID",  # Lucid Group
        "ABNB",  # Airbnb Inc.
        "EXPE",  # Expedia Group
        "MAR",   # Marriott International
        "HLT",   # Hilton Worldwide
        "RCL",   # Royal Caribbean
        "CCL",   # Carnival Corp.
        "NCLH",  # Norwegian Cruise Line
        "LVS",   # Las Vegas Sands
        "MGM",   # MGM Resorts
        "WYNN",  # Wynn Resorts
        "DRI",   # Darden Restaurants
        "YUM",   # Yum! Brands
        "QSR",   # Restaurant Brands Intl.
        "DKNG",  # DraftKings Inc.
        "PENN",  # PENN Entertainment
        "RL",    # Ralph Lauren
        "PVH",   # PVH Corp.
        "TPR",   # Tapestry Inc.
        "VFC",   # VF Corp.
        "HBI",   # Hanesbrands
        "URBN",  # Urban Outfitters
        "ROST",  # Ross Stores
        "BBWI",  # Bath & Body Works
        "W",     # Wayfair Inc.
        "ETSY",  # Etsy Inc.
        "EBAY",  # eBay Inc.
        "AN",    # AutoNation Inc.
        "KMX",   # CarMax Inc.
        "CVNA",  # Carvana Co.
        "CAR",   # Avis Budget Group
        "HMC",   # Honda Motor Co.
        "TM",    # Toyota Motor Corp.
        "STLA",  # Stellantis N.V.
        "BWA",   # BorgWarner Inc.
        "LEA",   # Lear Corp.
        "MGA",   # Magna International
        "APTV",  # Aptiv PLC
    ],

    # ── 3. HEALTH CARE ─────────────────────────────────────────────────────
    "Health Care": [
        "LLY",   # Eli Lilly
        "UNH",   # UnitedHealth Group
        "JNJ",   # Johnson & Johnson
        "ABBV",  # AbbVie Inc.
        "MRK",   # Merck & Co.
        "PFE",   # Pfizer Inc.
        "ABT",   # Abbott Laboratories
        "TMO",   # Thermo Fisher Scientific
        "DHR",   # Danaher Corp.
        "SYK",   # Stryker Corp.
        "BSX",   # Boston Scientific
        "ISRG",  # Intuitive Surgical
        "MDT",   # Medtronic PLC
        "EW",    # Edwards Lifesciences
        "ZBH",   # Zimmer Biomet
        "BDX",   # Becton Dickinson
        "BAX",   # Baxter International
        "RMD",   # ResMed Inc.
        "HOLX",  # Hologic Inc.
        "IDXX",  # IDEXX Laboratories
        "IQV",   # IQVIA Holdings
        "CRL",   # Charles River Labs
        "A",     # Agilent Technologies
        "WAT",   # Waters Corp.
        "MTD",   # Mettler-Toledo
        "REGN",  # Regeneron Pharmaceuticals
        "VRTX",  # Vertex Pharmaceuticals
        "BIIB",  # Biogen Inc.
        "GILD",  # Gilead Sciences
        "AMGN",  # Amgen Inc.
        "BMY",   # Bristol-Myers Squibb
        "AZN",   # AstraZeneca PLC
        "NVO",   # Novo Nordisk A/S
        "MRNA",  # Moderna Inc.
        "BNTX",  # BioNTech SE
        "CVS",   # CVS Health
        "CI",    # Cigna Group
        "HUM",   # Humana Inc.
        "CNC",   # Centene Corp.
        "MOH",   # Molina Healthcare
        "MCK",   # McKesson Corp.
        "CAH",   # Cardinal Health
        "ABC",   # AmerisourceBergen
        "VEEV",  # Veeva Systems
        "DOCS",  # Doximity Inc.
        "ACAD",  # ACADIA Pharmaceuticals
        "INCY",  # Incyte Corp.
        "HALO",  # Halozyme Therapeutics
        "EXAS",  # Exact Sciences
        "NTRA",  # Natera Inc.
    ],

    # ── 4. FINANCIALS ──────────────────────────────────────────────────────
    "Financials": [
        "BRK.B", # Berkshire Hathaway B
        "JPM",   # JPMorgan Chase
        "BAC",   # Bank of America
        "WFC",   # Wells Fargo
        "GS",    # Goldman Sachs
        "MS",    # Morgan Stanley
        "BLK",   # BlackRock Inc.
        "C",     # Citigroup Inc.
        "SCHW",  # Charles Schwab
        "AXP",   # American Express
        "V",     # Visa Inc.
        "MA",    # Mastercard
        "PYPL",  # PayPal Holdings
        "SQ",    # Block Inc. (Square)
        "COF",   # Capital One Financial
        "USB",   # U.S. Bancorp
        "PNC",   # PNC Financial Services
        "TFC",   # Truist Financial
        "KEY",   # KeyCorp
        "RF",    # Regions Financial
        "HBAN",  # Huntington Bancshares
        "CFG",   # Citizens Financial
        "MTB",   # M&T Bank
        "FITB",  # Fifth Third Bancorp
        "CMA",   # Comerica Inc.
        "ZION",  # Zions Bancorporation
        "ALLY",  # Ally Financial
        "SYF",   # Synchrony Financial
        "DFS",   # Discover Financial
        "AIG",   # American Intl. Group
        "MET",   # MetLife Inc.
        "PRU",   # Prudential Financial
        "AFL",   # Aflac Inc.
        "ALL",   # Allstate Corp.
        "TRV",   # Travelers Companies
        "CB",    # Chubb Ltd.
        "HIG",   # Hartford Financial
        "L",     # Loews Corp.
        "GL",    # Globe Life
        "LNC",   # Lincoln National
        "CINF",  # Cincinnati Financial
        "RE",    # Everest Re Group
        "RNR",   # RenaissanceRe Holdings
        "SPGI",  # S&P Global
        "MCO",   # Moody's Corp.
        "ICE",   # Intercontinental Exchange
        "CME",   # CME Group
        "CBOE",  # Cboe Global Markets
        "NDAQ",  # Nasdaq Inc.
        "FDS",   # FactSet Research
        "MSCI",  # MSCI Inc.
    ],

    # ── 5. COMMUNICATION SERVICES ──────────────────────────────────────────
    "Communication Services": [
        "NFLX",  # Netflix Inc.
        "DIS",   # Walt Disney Co.
        "CMCSA", # Comcast Corp.
        "T",     # AT&T Inc.
        "VZ",    # Verizon Communications
        "TMUS",  # T-Mobile US
        "CHTR",  # Charter Communications
        "PARA",  # Paramount Global
        "WBD",   # Warner Bros. Discovery
        "FOX",   # Fox Corp. (Class B)
        "FOXA",  # Fox Corp. (Class A)
        "NYT",   # New York Times
        "NWSA",  # News Corp (Class A)
        "IAC",   # IAC Inc.
        "ZM",    # Zoom Video Communications
        "SNAP",  # Snap Inc.
        "PINS",  # Pinterest Inc.
        "RDDT",  # Reddit Inc.
        "SPOT",  # Spotify Technology
        "TTD",   # The Trade Desk
        "MGNI",  # Magnite Inc.
        "PUBM",  # PubMatic Inc.
        "APPS",  # Digital Turbine
        "LUMN",  # Lumen Technologies
        "SIRI",  # Sirius XM Holdings
        "AMC",   # AMC Networks
        "LYV",   # Live Nation Entertainment
        "EA",    # Electronic Arts
        "TTWO",  # Take-Two Interactive
        "RBLX",  # Roblox Corp.
        "U",     # Unity Software
        "MTCH",  # Match Group
        "BMBL",  # Bumble Inc.
        "LYFT",  # Lyft Inc.
        "UBER",  # Uber Technologies
        "DASH",  # DoorDash Inc.
    ],

    # ── 6. INDUSTRIALS ─────────────────────────────────────────────────────
    "Industrials": [
        "RTX",   # RTX Corp. (Raytheon)
        "HON",   # Honeywell International
        "UPS",   # United Parcel Service
        "BA",    # Boeing Co.
        "CAT",   # Caterpillar Inc.
        "DE",    # Deere & Company
        "GE",    # GE Aerospace
        "LMT",   # Lockheed Martin
        "NOC",   # Northrop Grumman
        "GD",    # General Dynamics
        "MMM",   # 3M Company
        "EMR",   # Emerson Electric
        "ROK",   # Rockwell Automation
        "ETN",   # Eaton Corp.
        "PH",    # Parker Hannifin
        "DOV",   # Dover Corp.
        "ITW",   # Illinois Tool Works
        "AME",   # AMETEK Inc.
        "XYL",   # Xylem Inc.
        "ROP",   # Roper Technologies
        "VRSK",  # Verisk Analytics
        "CTAS",  # Cintas Corp.
        "RSG",   # Republic Services
        "WM",    # Waste Management
        "EXPD",  # Expeditors International
        "FDX",   # FedEx Corp.
        "CHRW",  # C.H. Robinson
        "JBHT",  # J.B. Hunt Transport
        "XPO",   # XPO Inc.
        "SAIA",  # Saia Inc.
        "ODFL",  # Old Dominion Freight
        "WAB",   # Wabtec Corp.
        "CSX",   # CSX Corp.
        "NSC",   # Norfolk Southern
        "UNP",   # Union Pacific
        "DAL",   # Delta Air Lines
        "UAL",   # United Airlines
        "AAL",   # American Airlines
        "LUV",   # Southwest Airlines
        "ALK",   # Alaska Air Group
        "SAVE",  # Spirit Airlines
        "JOBY",  # Joby Aviation
        "HWM",   # Howmet Aerospace
        "SPR",   # Spirit AeroSystems
        "TDG",   # TransDigm Group
        "AXON",  # Axon Enterprise
        "MSA",   # MSA Safety
        "GNRC",  # Generac Holdings
        "AOS",   # A.O. Smith
        "MAS",   # Masco Corp.
    ],

    # ── 7. ENERGY ──────────────────────────────────────────────────────────
    "Energy": [
        "XOM",   # ExxonMobil
        "CVX",   # Chevron Corp.
        "COP",   # ConocoPhillips
        "EOG",   # EOG Resources
        "SLB",   # SLB (Schlumberger)
        "MPC",   # Marathon Petroleum
        "PSX",   # Phillips 66
        "VLO",   # Valero Energy
        "OXY",   # Occidental Petroleum
        "PXD",   # Pioneer Natural Resources
        "DVN",   # Devon Energy
        "FANG",  # Diamondback Energy
        "HAL",   # Halliburton Co.
        "BKR",   # Baker Hughes
        "NOV",   # NOV Inc.
        "HES",   # Hess Corp.
        "MRO",   # Marathon Oil
        "APA",   # APA Corp.
        "CTRA",  # Coterra Energy
        "EQT",   # EQT Corp.
        "RRC",   # Range Resources
        "AR",    # Antero Resources
        "CNX",   # CNX Resources
        "SM",    # SM Energy
        "MTDR",  # Matador Resources
        "CHRD",  # Chord Energy
        "OVV",   # Ovintiv Inc.
        "PR",    # Permian Resources
        "WMB",   # Williams Companies
        "KMI",   # Kinder Morgan
        "ET",    # Energy Transfer
        "EPD",   # Enterprise Products
        "MMP",   # Magellan Midstream
        "PAA",   # Plains All American
        "TRGP",  # Targa Resources
        "DINO",  # HF Sinclair
        "PBF",   # PBF Energy
        "DKL",   # Delek Logistics
        "CLNE",  # Clean Energy Fuels
        "BE",    # Bloom Energy
        "PLUG",  # Plug Power
        "FCEL",  # FuelCell Energy
    ],

    # ── 8. CONSUMER STAPLES ────────────────────────────────────────────────
    "Consumer Staples": [
        "PG",    # Procter & Gamble
        "KO",    # Coca-Cola Co.
        "PEP",   # PepsiCo Inc.
        "COST",  # Costco Wholesale
        "WMT",   # Walmart Inc.
        "PM",    # Philip Morris Intl.
        "MO",    # Altria Group
        "EL",    # Estee Lauder
        "CL",    # Colgate-Palmolive
        "KMB",   # Kimberly-Clark
        "CHD",   # Church & Dwight
        "CLX",   # Clorox Co.
        "HRL",   # Hormel Foods
        "SJM",   # J.M. Smucker
        "MKC",   # McCormick & Co.
        "CPB",   # Campbell Soup
        "GIS",   # General Mills
        "K",     # Kellanova (Kellogg)
        "CAG",   # Conagra Brands
        "HSY",   # Hershey Co.
        "MDLZ",  # Mondelez International
        "KHC",   # Kraft Heinz
        "TSN",   # Tyson Foods
        "BG",    # Bunge Global
        "ADM",   # Archer-Daniels-Midland
        "SYY",   # Sysco Corp.
        "USM",   # US Foods Holding
        "KR",    # Kroger Co.
        "ACI",   # Albertsons Companies
        "GO",    # Grocery Outlet
        "SPTN",  # SpartanNash Co.
        "CHEF",  # Chefs' Warehouse
        "COKE",  # Coca-Cola Consolidated
        "CELH",  # Celsius Holdings
        "MNST",  # Monster Beverage
        "SAM",   # Boston Beer Co.
        "BUD",   # Anheuser-Busch InBev
        "TAP",   # Molson Coors
        "STZ",   # Constellation Brands
        "DXCM",  # DexCom Inc.
    ],

    # ── 9. REAL ESTATE (REITs) ─────────────────────────────────────────────
    "Real Estate": [
        "AMT",   # American Tower
        "PLD",   # Prologis Inc.
        "EQIX",  # Equinix Inc.
        "CCI",   # Crown Castle
        "SPG",   # Simon Property Group
        "O",     # Realty Income
        "WELL",  # Welltower Inc.
        "DLR",   # Digital Realty
        "AVB",   # AvalonBay Communities
        "EQR",   # Equity Residential
        "INVH",  # Invitation Homes
        "AMH",   # American Homes 4 Rent
        "IRM",   # Iron Mountain
        "WY",    # Weyerhaeuser Co.
        "PSA",   # Public Storage
        "EXR",   # Extra Space Storage
        "CUBE",  # CubeSmart
        "NNN",   # NNN REIT
        "STAG",  # STAG Industrial
        "COLD",  # Americold Realty
        "EGP",   # EastGroup Properties
        "FR",    # First Industrial Realty
        "REXR",  # Rexford Industrial
        "KIM",   # Kimco Realty
        "REG",   # Regency Centers
        "FRT",   # Federal Realty
        "MAC",   # Macerich Co.
        "CBL",   # CBL & Associates
        "VTR",   # Ventas Inc.
        "PEAK",  # Healthpeak Properties
        "HR",    # Healthcare Realty
        "DOC",   # Physicians Realty
        "SBAC",  # SBA Communications
        "UNIT",  # Uniti Group
    ],

    # ── 10. UTILITIES ──────────────────────────────────────────────────────
    "Utilities": [
        "NEE",   # NextEra Energy
        "DUK",   # Duke Energy
        "SO",    # Southern Company
        "D",     # Dominion Energy
        "SRE",   # Sempra Energy
        "AEP",   # American Electric Power
        "XEL",   # Xcel Energy
        "PCG",   # PG&E Corp.
        "EXC",   # Exelon Corp.
        "ED",    # Consolidated Edison
        "ES",    # Eversource Energy
        "ETR",   # Entergy Corp.
        "WEC",   # WEC Energy Group
        "CMS",   # CMS Energy
        "LNT",   # Alliant Energy
        "EVRG",  # Evergy Inc.
        "OGE",   # OGE Energy
        "NWE",   # NorthWestern Energy
        "POR",   # Portland General Electric
        "AVA",   # Avista Corp.
        "IDA",   # IDACORP Inc.
        "ATO",   # Atmos Energy
        "NI",    # NiSource Inc.
        "SWX",   # Southwest Gas Holdings
        "NEW",   # Paysign Inc.
        "AWK",   # American Water Works
        "WTRG",  # Essential Utilities
        "SJW",   # SJW Group
        "MSEX",  # Middlesex Water
        "GWRS",  # Global Water Resources
    ],

    # ── 11. MATERIALS ──────────────────────────────────────────────────────
    "Materials": [
        "LIN",   # Linde PLC
        "APD",   # Air Products & Chemicals
        "SHW",   # Sherwin-Williams
        "ECL",   # Ecolab Inc.
        "NEM",   # Newmont Corp.
        "FCX",   # Freeport-McMoRan
        "NUE",   # Nucor Corp.
        "STLD",  # Steel Dynamics
        "X",     # U.S. Steel
        "CLF",   # Cleveland-Cliffs
        "AA",    # Alcoa Corp.
        "CENX",  # Century Aluminum
        "ATI",   # ATI Inc.
        "CRS",   # Carpenter Technology
        "TS",    # Tenaris S.A.
        "VMC",   # Vulcan Materials
        "MLM",   # Martin Marietta Materials
        "SLGN",  # Silgan Holdings
        "SEE",   # Sealed Air Corp.
        "BALL",  # Ball Corp.
        "IP",    # International Paper
        "PKG",   # Packaging Corp. of America
        "WRK",   # WestRock Co.
        "GPK",   # Graphic Packaging
        "CCK",   # Crown Holdings
        "OLN",   # Olin Corp.
        "CE",    # Celanese Corp.
        "EMN",   # Eastman Chemical
        "LYB",   # LyondellBasell
        "HUN",   # Huntsman Corp.
        "RPM",   # RPM International
        "PPG",   # PPG Industries
        "IFF",   # Intl. Flavors & Fragrances
        "FMC",   # FMC Corp.
        "MOS",   # Mosaic Co.
        "CF",    # CF Industries
        "SMG",   # Scotts Miracle-Gro
        "CTVA",  # Corteva Inc.
    ],

    # ── 12. ADDITIONAL HIGH-GROWTH / POPULAR ──────────────────────────────
    "High Growth & Popular": [
        # Fintech / Payments
        "AFRM",  # Affirm Holdings
        "UPST",  # Upstart Holdings
        "SOFI",  # SoFi Technologies
        "HOOD",  # Robinhood Markets
        "OPEN",  # Opendoor Technologies
        "COOP",  # Mr. Cooper Group
        "PFSI",  # PennyMac Financial
        "RDFN",  # Redfin Corp.
        "Z",     # Zillow Group
        # EV / Clean Energy / Space
        "CHPT",  # ChargePoint Holdings
        "BLNK",  # Blink Charging
        "EVGO",  # EVgo Inc.
        "ENVX",  # Enovix Corp.
        "SPCE",  # Virgin Galactic
        "RKLB",  # Rocket Lab USA
        "ASTR",  # Astra Space
        "MNTS",  # Momentus Inc.
        "RDW",   # Redwire Corp.
        "LHX",   # L3Harris Technologies
        "KTOS",  # Kratos Defense
        "CACI",  # CACI International
        "LDOS",  # Leidos Holdings
        "BAH",   # Booz Allen Hamilton
        "SAIC",  # Science Applications Intl.
        # SaaS / Cloud
        "ZI",    # ZoomInfo Technologies
        "HUBS",  # HubSpot Inc.
        "BILL",  # BILL Holdings
        "MDB",   # MongoDB Inc.
        "CFLT",  # Confluent Inc.
        "GTLB",  # GitLab Inc.
        "ESTC",  # Elastic N.V.
        "SUMO",  # Sumo Logic
        "APPN",  # Appian Corp.
        "PEGA",  # Pegasystems
        "NCNO",  # nCino Inc.
        "ALTR",  # Altair Engineering
        "MSTR",  # MicroStrategy
        "COIN",  # Coinbase Global
        "MARA",  # Marathon Digital Holdings
        "RIOT",  # Riot Platforms
        "CLSK",  # CleanSpark
        "HUT",   # Hut 8 Corp.
        # Healthcare Innovation
        "RXRX",  # Recursion Pharmaceuticals
        "SDGR",  # Schrödinger Inc.
        "ABCL",  # AbCellera Biologics
        "BEAM",  # Beam Therapeutics
        "CRSP",  # CRISPR Therapeutics
        "EDIT",  # Editas Medicine
        "NTLA",  # Intellia Therapeutics
        "FATE",  # Fate Therapeutics
        "KYMR",  # Kymera Therapeutics
        "PRAX",  # Praxis Precision Medicine
        "ARWR",  # Arrowhead Pharmaceuticals
        "ALNY",  # Alnylam Pharmaceuticals
        "IONS",  # Ionis Pharmaceuticals
        "SRPT",  # Sarepta Therapeutics
        "RARE",  # Ultragenyx Pharmaceutical
        "FOLD",  # Amicus Therapeutics
        "ACMR",  # ACM Research
        "OLLI",  # Ollie's Bargain Outlet
        "FIVE",  # Five Below
        "BJ",    # BJ's Wholesale Club
        "DLTR",  # Dollar Tree
        "DG",    # Dollar General
        "PRGO",  # Perrigo Co.
        "AMPH",  # Amphastar Pharmaceuticals
        "SUPN",  # Supernus Pharmaceuticals
        "IBRX",  # ImmunityBio Inc.
        "INO",   # Inovio Pharmaceuticals
        "NVAX",  # Novavax Inc.
        "VXRT",  # Vaxart Inc.
        "OCGN",  # Ocugen Inc.
        "SAVA",  # Cassava Sciences
        "AGIO",  # Agios Pharmaceuticals
        "PTGX",  # Protagonist Therapeutics
        "XNCR",  # Xencor Inc.
        "ALLK",  # Allakos Inc.
        "RCKT",  # Rocket Pharmaceuticals
        "PGEN",  # Precigen Inc.
        "VERV",  # Verve Therapeutics
        "TNGX",  # Tango Therapeutics
        "RVMD",  # Revolution Medicines
        "IMVT",  # Immunovant Inc.
        "ACVA",  # ACV Auctions
        "GCMG",  # GCM Grosvenor
        "FROG",  # JFrog Ltd.
        "DOCN",  # DigitalOcean Holdings
        "FSLY",  # Fastly Inc.
        "BAND",  # Bandwidth Inc.
        "TWLO",  # Twilio Inc.
        "MSGM",  # Motorsport Games
        "LAZR",  # Luminar Technologies
        "MOBILEYE", # Mobileye Global
        "INDI",  # indie Semiconductor
        "OUST",  # Ouster Inc.
        "INVZ",  # Innoviz Technologies
        "VLDR",  # Velodyne Lidar
        "LIDR",  # AEye Inc.
        "NKLA",  # Nikola Corp.
        "HYZN",  # Hyzon Motors
        "WKHS",  # Workhorse Group
        "RIDE",  # Lordstown Motors
    ],

    "Index": [
        "SPX",
        "VIX",
    ]
}

IDX_STOCKS = {
    "IDX30": [
        "ADRO",
        "AMRT",
        "ASII",
        "BBCA",
        "BBNI",
        "BBRI",
        "AADI",
        "BMRI",
        "ANTM",
        "BRPT",
        "BUMI",
        "CPIN",
        "EMTK",
        "ISAT",
        "GOTO",
        "JPFA",
        "ICBP",
        "INCO",
        "INDF",
        "INKP",
        "MBMA",
        "MEDC",
        "KLBF",
        "PGEO",
        "MDKA",
        "PGAS",
        "PTBA",
        "UNTR",
        "UNVR",
        "TLKM",
    ]
}

COMMODITY = {
    "Gold/Silver/Oil": [
        "AU",
        "AG",
        "CL",
    ]
}

FOREX = {
    "Usd/Eur/Gbp": [
        "DXY",
        "USD/IDR",
        "EUR/USD",
        "GBP/USD",
        "AUD/USD",
        "NZD/USD",
        "USD/JPY",
        "USD/CAD",
        "USD/CHF",
        "EUR/AUD",
        "EUR/CAD",
        "EUR/CHF",
        "EUR/GBP",
        "EUR/JPY",
        "EUR/NZD",
        "GBP/AUD",
        "GBP/CAD",
        "GBP/CHF",
        "GBP/JPY",
        "GBP/NZD",
    ]
}