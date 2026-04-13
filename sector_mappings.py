"""
Sector and Product/Domain Mapping Configuration
This file contains mappings used for sector inference and validation in the job description analysis system.
"""

# Product/domain keyword mapping for second sector validation
# Maps product keywords to valid domain keywords they should match
PRODUCT_TO_DOMAIN_KEYWORDS = {
    # Consumer & Retail
    "mobile phone": ["consumer electronics", "electronics"],
    "smartphone": ["consumer electronics", "electronics"],
    "phone": ["consumer electronics", "electronics"],
    "tablet": ["consumer electronics", "electronics"],
    "laptop": ["consumer electronics", "electronics"],
    "wearables": ["consumer electronics", "electronics"],
    "fashion": ["fashion & apparel", "retail"],
    "clothing": ["fashion & apparel", "retail"],
    "food": ["food & beverage", "retail"],
    "beverage": ["food & beverage", "retail"],
    "luxury": ["luxury goods", "retail"],
    "e-commerce": ["e-commerce", "retail"],
    "ecommerce": ["e-commerce", "retail"],
    "retail": ["retail", "e-commerce"],

    # Technology
    "cloud": ["cloud", "infrastructure"],
    "devops": ["cloud", "infrastructure"],
    "kubernetes": ["cloud", "infrastructure"],
    "aws": ["cloud", "infrastructure"],
    "azure": ["cloud", "infrastructure"],
    "gcp": ["cloud", "infrastructure"],
    "ai": ["ai", "data", "artificial intelligence"],
    "machine learning": ["ai", "data"],
    "data science": ["ai", "data"],
    "big data": ["ai", "data"],
    "software": ["software", "it services"],
    "web": ["software", "it services"],
    "app": ["software", "it services"],
    "hardware": ["hardware", "electronics"],
    "cybersecurity": ["cybersecurity", "security"],
    "security": ["cybersecurity", "security"],
    "network": ["it services", "cloud", "infrastructure"],

    # Media, Gaming & Entertainment
    "gaming": ["gaming", "entertainment"],
    "game": ["gaming", "entertainment"],
    "esports": ["gaming", "entertainment"],
    "streaming": ["streaming & ott", "entertainment"],
    "video": ["streaming & ott", "entertainment"],
    "publishing": ["publishing", "media"],
    "advertising": ["advertising & marketing", "media"],
    "marketing": ["advertising & marketing", "media"],
    "creator": ["creator economy", "media"],

    # Financial Services
    "fintech": ["fintech", "financial"],
    "bank": ["banking", "financial"],
    "insurance": ["insurance", "financial"],
    "investment": ["investment", "financial", "asset management"],
    "asset management": ["investment", "financial", "asset management"],
    "wealth": ["investment", "financial", "asset management"],
    "accounting": ["accounting & audit", "financial"],
    "audit": ["accounting & audit", "financial"],
    "compliance": ["legal & compliance", "financial"],
    "legal": ["legal & compliance", "financial"],

    # Healthcare
    "healthcare": ["healthcare", "healthtech"],
    "medical": ["healthcare", "medical"],
    "pharmaceutical": ["pharmaceuticals", "biotech"],
    "pharma": ["pharmaceuticals", "biotech"],
    "biotech": ["pharmaceuticals", "biotech", "biotechnology"],
    "clinical": ["clinical research", "healthcare"],
    "diagnostics": ["diagnostics", "healthcare"],
    "devices": ["medical devices", "healthcare"],
    "hospital": ["healthcare services", "healthcare"],

    # Industrial & Manufacturing
    "manufacturing": ["manufacturing", "machinery", "automotive"],
    "machinery": ["machinery", "manufacturing"],
    "automotive": ["automotive", "manufacturing"],
    "aerospace": ["aerospace & defense", "manufacturing"],
    "defense": ["aerospace & defense", "manufacturing"],
    "construction": ["construction materials", "manufacturing"],
    "chemicals": ["chemicals", "manufacturing"],
    "electronics": ["electronics", "manufacturing"],

    # Energy & Environment
    "energy": ["energy", "renewable energy", "oil & gas"],
    "renewable": ["renewable energy", "energy"],
    "solar": ["renewable energy", "energy"],
    "wind": ["renewable energy", "energy"],
    "oil & gas": ["oil & gas", "energy"],
    "natural gas": ["oil & gas", "energy"],
    "utilities": ["utilities", "energy"],
    "sustainability": ["esg & sustainability", "energy"],

    # Agriculture & Natural Resources
    "agriculture": ["agribusiness", "agriculture"],
    "farming": ["agribusiness", "agriculture"],
    "forestry": ["forestry", "natural resources"],
    "fisheries": ["fisheries", "natural resources"],
    "mining": ["mining", "natural resources"],

    # Transport & Infrastructure
    "aviation": ["aviation", "transport"],
    "airline": ["aviation", "transport"],
    "maritime": ["maritime", "transport"],
    "shipping": ["maritime", "transport"],
    "rail": ["rail & transit", "transport"],
    "transit": ["rail & transit", "transport"],
    "mobility": ["ride-hailing & mobility", "transport"],
    "smart cities": ["smart cities", "infrastructure"],

    # Government, Education & Non-Profit
    "public sector": ["public sector", "government"],
    "education": ["education", "government"],
    "school": ["education", "government"],
    "university": ["education", "government"],
    "ngo": ["ngos & charities", "non-profit"],
    "charity": ["ngos & charities", "non-profit"],
    "security": ["defense & security", "government"],

    # Emerging & Cross-Sector
    "web3": ["web3", "blockchain"],
    "blockchain": ["web3", "blockchain"],
    "crypto": ["web3", "blockchain"],
    "nft": ["web3", "blockchain"],
    "agentic ai": ["agentic ai & automation", "ai"],
    "automation": ["agentic ai & automation", "technology"],
    "spacetech": ["spacetech", "emerging"],
    "satellite": ["spacetech", "emerging"],
    "influencer": ["creator & influencer platforms", "media"],
    "localization": ["gaming localization", "gaming"]
}

# Generic role keywords that can match any sector when no specific product is found
GENERIC_ROLE_KEYWORDS = [
    "manager", 
    "engineer", 
    "developer", 
    "analyst", 
    "consultant", 
    "director", 
    "lead", 
    "specialist", 
    "coordinator", 
    "administrator",
    "associate",
    "executive",
    "officer",
    "advisor",
    "strategist"
]

# BUCKET_COMPANIES extended with financial_services bucket and other existing buckets
BUCKET_COMPANIES = {
    "pharma_biotech": {
        "global": ["Pfizer", "Roche", "Novartis", "Johnson & Johnson", "Merck", "GSK", "Sanofi", "AstraZeneca", "Bayer"],
        "apac": ["Takeda", "CSL", "Sino Biopharm", "Sun Pharma", "Daiichi Sankyo"]
    },
    "medical_devices": {
        "global": ["Johnson & Johnson", "Medtronic", "Abbott", "Baxter", "Stryker", "BD", "Philips Healthcare", "Siemens Healthineers"],
        "apac": ["Terumo", "Nipro", "Wuxi AppTec (Devices)"]
    },
    "diagnostics": {
        "global": ["Roche Diagnostics", "Siemens Healthineers", "Abbott Diagnostics", "BD", "Qiagen", "Bio-Rad"],
        "apac": ["Sysmex", "Mindray"]
    },
    "clinical_research": {
        "global": ["IQVIA", "Labcorp", "ICON", "Parexel", "PPD", "Syneos Health"],
        "apac": ["Novotech", "Tigermed"]
    },
    "healthtech": {
        "global": ["Philips", "Siemens Healthineers", "GE HealthCare", "Cerner (Oracle Health)", "Epic Systems"],
        "apac": ["HealthHub", "IHiS", "Ramsay Sime Darby Health Care"]
    },
    "technology": {
        "global": ["Microsoft", "Amazon Web Services", "Google Cloud", "Snowflake", "Databricks"],
        "apac": ["Tencent Cloud", "Alibaba Cloud"]
    },
    "manufacturing": {
        "global": ["Siemens", "ABB", "Rockwell Automation", "Schneider Electric", "Bosch"],
        "apac": ["Mitsubishi Electric", "FANUC", "Yaskawa"]
    },
    "energy": {
        "global": ["Shell", "BP", "TotalEnergies", "Schneider Electric", "Siemens Energy"],
        "apac": ["PETRONAS", "Sembcorp", "Keppel"]
    },
    "gaming": {
        "global": ["Sony Interactive Entertainment", "Ubisoft", "Electronic Arts", "Nintendo", "Activision Blizzard"],
        "apac": ["Tencent", "NetEase", "Bandai Namco"]
    },
    "web3": {
        "global": ["Coinbase", "Consensys", "Binance", "Circle"],
        "apac": ["OKX", "Bybit"]
    },
    # New: Financial Services bucket to align with sectors.json "Financial Services > ..."
    "financial_services": {
        "global": [
            "J.P. Morgan", "Goldman Sachs", "Morgan Stanley", "BlackRock", "UBS", "Credit Suisse", "HSBC", "Citi", "BNP Paribas", "Deutsche Bank",
            "Standard Chartered", "State Street", "Northern Trust", "Schroders", "Fidelity"
        ],
        "apac": [
            "Samsung Life Insurance", "Hana Financial Investment", "Mirae Asset", "KB Asset Management", "NH Investment & Securities",
            "Korea Investment & Securities", "Shinhan Investment Corp", "Samsung Securities", "Samsung Fire & Marine Insurance", "Hyundai Marine & Fire Insurance",
            "DB Insurance", "Meritz Fire & Marine Insurance", "Tong Yang Securities", "Woori Investment Bank", "Daishin Securities", "Hana Securities",
            "Kiwoom Securities", "KTB Investment & Securities", "Eugene Investment & Securities", "Korea Life Insurance", "LSM Investment",
            "Shinhan BNP Paribas Asset Management", "Samsung SDS", "LG CNS", "SK C&C", "POSCO ICT", "Hyundai Information Technology", "Hanmi Financial",
            "Nonghyup Bank", "Lotte Card"
        ]
    }
}

BUCKET_JOB_TITLES = {
    "pharma_biotech": [
        "Regulatory Affairs Manager", "Clinical Research Associate", "Pharmacovigilance Specialist",
        "Medical Affairs Manager", "Quality Assurance Specialist", "CMC Scientist",
        "Biostatistician", "Clinical Project Manager", "Drug Safety Officer",
        "Clinical Data Manager", "Medical Science Liaison", "Toxicologist",
        "Regulatory Affairs Specialist", "Clinical Operations Manager"
    ],
    "medical_devices": [
        "Regulatory Affairs Manager", "Quality Engineer", "Clinical Affairs Specialist",
        "Design Control Engineer", "Risk Management Engineer", "Product Manager (Medical Device)",
        "Manufacturing Engineer", "Validation Engineer", "Biomedical Engineer",
        "Device Development Scientist", "Clinical Evaluation Specialist"
    ],
    "diagnostics": [
        "IVD Regulatory Specialist", "Quality Systems Engineer", "Clinical Application Specialist",
        "Assay Development Scientist", "Validation Engineer", "Molecular Diagnostics Scientist",
        "Lab Automation Engineer", "Clinical Laboratory Scientist"
    ],
    "clinical_research": [
        "CRA", "Senior CRA", "Clinical Project Manager", "Clinical Trial Manager",
        "Study Start-Up Specialist", "Site Activation Manager", "Regulatory Start-Up Specialist",
        "Clinical Operations Lead", "Clinical Program Manager"
    ],
    "healthtech": [
        "Clinical Informatics Lead", "Healthcare Data Scientist",
        "Interoperability Engineer", "Implementation Consultant", "Digital Health Analyst",
        "Telemedicine Product Manager", "HealthTech Solutions Architect",
        "Electronic Health Records Specialist"
    ],
    "technology": [
        "Software Engineer", "ML Engineer", "Data Scientist", "Solutions Architect",
        "Security Engineer", "MLOps Engineer", "Cloud Engineer", "DevOps Engineer",
        "AI Research Scientist", "Backend Developer", "Frontend Developer",
        "Full Stack Engineer", "Systems Administrator", "IT Project Manager"
    ],
    "manufacturing": [
        "Manufacturing Engineer", "Quality Engineer", "Process Engineer",
        "Supply Chain Analyst", "Automation Engineer", "Industrial Engineer",
        "Lean Manufacturing Specialist", "Operations Manager", "Production Planner"
    ],
    "energy": [
        "Energy Analyst", "Grid Integration Engineer", "Sustainability Manager",
        "HSE Engineer", "Renewable Energy Project Manager", "Solar Engineer",
        "Wind Energy Specialist", "Oil & Gas Engineer", "Utilities Operations Manager"
    ],
    "gaming": [
        "Game Producer", "Gameplay Engineer", "Level Designer", "Technical Artist",
        "Game Tester", "Game QA Engineer", "Narrative Designer", "Game Economy Designer",
        "Esports Manager"
    ],
    "web3": [
        "Blockchain Engineer", "Smart Contract Developer", "Web3 Product Manager",
        "Crypto Analyst", "NFT Product Manager", "Decentralized App (dApp) Developer",
        "Tokenomics Specialist"
    ],
    "financial_services": [
        "Investment Analyst", "Product Manager (Wealth/Investment)", "Portfolio Manager",
        "Risk Analyst", "Payments Product Manager", "Fintech Product Manager",
        "Relationship Manager", "Asset Manager", "Compliance Officer",
        "Banking Operations Manager", "Insurance Underwriter", "Credit Risk Analyst"
    ],
    "government_education_nonprofit": [
        "Policy Analyst", "Public Sector Project Manager", "Education Program Manager",
        "University Research Coordinator", "NGO Program Officer", "Charity Fundraising Manager",
        "Defense Analyst", "Security Policy Advisor"
    ],
    "transport_infrastructure": [
        "Aviation Operations Manager", "Rail Project Engineer", "Mobility Product Manager",
        "Smart Cities Consultant", "Maritime Logistics Coordinator", "Transit Planner"
    ],
    "other": [
        "Project Manager", "Operations Manager", "Business Analyst", "Data Analyst",
        "HR Manager", "Recruitment Consultant", "Marketing Manager", "Sales Manager",
        "Customer Success Manager"
    ]
}