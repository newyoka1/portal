#!/usr/bin/env python3
"""
Ethnicity Prediction and Standardization Module
Uses Census surname data and name patterns for ethnicity prediction
"""
import re
from typing import Optional

# Surname to Ethnicity Mapping - Expanded with European Origins
# Format: {surname: primary_ethnicity_code}
# Codes: H=Hispanic, A=Asian, B=Black, CH=Chinese, IT=Italian, RU=Russian, IR=Irish
SURNAME_ETHNICITY_MAP = {
    # Hispanic/Latino indicators
    'GARCIA': 'H', 'RODRIGUEZ': 'H', 'MARTINEZ': 'H', 'HERNANDEZ': 'H', 'LOPEZ': 'H',
    'GONZALEZ': 'H', 'PEREZ': 'H', 'SANCHEZ': 'H', 'RAMIREZ': 'H', 'TORRES': 'H',
    'RIVERA': 'H', 'GOMEZ': 'H', 'DIAZ': 'H', 'REYES': 'H', 'CRUZ': 'H',
    'MORALES': 'H', 'GUTIERREZ': 'H', 'ORTIZ': 'H', 'JIMENEZ': 'H', 'RUIZ': 'H',
    'MENDOZA': 'H', 'ALVAREZ': 'H', 'CASTILLO': 'H', 'ROMERO': 'H', 'HERRERA': 'H',
    'VASQUEZ': 'H', 'MEDINA': 'H', 'VARGAS': 'H', 'CASTRO': 'H', 'RAMOS': 'H',
    'FLORES': 'H', 'CHAVEZ': 'H', 'SOTO': 'H', 'RIOS': 'H', 'MORENO': 'H',
    'AGUILAR': 'H', 'GUZMAN': 'H', 'ROJAS': 'H', 'CONTRERAS': 'H', 'SILVA': 'H',
    
    # Chinese surnames - most common
    'WANG': 'CH', 'LI': 'CH', 'ZHANG': 'CH', 'LIU': 'CH', 'CHEN': 'CH',
    'YANG': 'CH', 'HUANG': 'CH', 'ZHAO': 'CH', 'WU': 'CH', 'ZHOU': 'CH',
    'XU': 'CH', 'SUN': 'CH', 'MA': 'CH', 'ZHU': 'CH', 'HU': 'CH',
    'GUO': 'CH', 'HE': 'CH', 'GAO': 'CH', 'LIN': 'CH', 'LUO': 'CH',
    'ZHENG': 'CH', 'LIANG': 'CH', 'SONG': 'CH', 'TANG': 'CH', 'XU': 'CH',
    'HAN': 'CH', 'FENG': 'CH', 'DENG': 'CH', 'CAO': 'CH', 'PENG': 'CH',
    'ZENG': 'CH', 'XIAO': 'CH', 'TIAN': 'CH', 'DONG': 'CH', 'PAN': 'CH',
    
    # Other Asian (Vietnamese, Korean, Indian, Japanese)
    'NGUYEN': 'A', 'TRAN': 'A', 'LE': 'A', 'PHAM': 'A', 'HOANG': 'A',
    'VO': 'A', 'DANG': 'A', 'BUI': 'A', 'DO': 'A', 'DUONG': 'A',
    'PATEL': 'A', 'SINGH': 'A', 'KUMAR': 'A', 'SHARMA': 'A', 'SHAH': 'A',
    'KIM': 'A', 'PARK': 'A', 'LEE': 'A', 'CHOI': 'A', 'JUNG': 'A',
    'KANG': 'A', 'CHO': 'A', 'YUN': 'A', 'JANG': 'A', 'LIM': 'A',
    'TAKAHASHI': 'A', 'TANAKA': 'A', 'WATANABE': 'A', 'SUZUKI': 'A', 'SATO': 'A',
    'ITO': 'A', 'YAMAMOTO': 'A', 'NAKAMURA': 'A', 'KOBAYASHI': 'A', 'KATO': 'A',
    
    # Italian surnames - most common
    'RUSSO': 'IT', 'FERRARI': 'IT', 'ESPOSITO': 'IT', 'BIANCHI': 'IT', 'ROMANO': 'IT',
    'COLOMBO': 'IT', 'RICCI': 'IT', 'MARINO': 'IT', 'GRECO': 'IT', 'BRUNO': 'IT',
    'GALLO': 'IT', 'CONTI': 'IT', 'DEFAZIO': 'IT', 'COSTA': 'IT', 'GIORDANO': 'IT',
    'MANCINI': 'IT', 'RIZZO': 'IT', 'LOMBARDI': 'IT', 'MORETTI': 'IT', 'BARBIERI': 'IT',
    'FONTANA': 'IT', 'SANTORO': 'IT', 'MARINI': 'IT', 'RINALDI': 'IT', 'CARUSO': 'IT',
    'FERRARA': 'IT', 'GALLI': 'IT', 'MARTINI': 'IT', 'LEONE': 'IT', 'LONGO': 'IT',
    'GENTILE': 'IT', 'MARTINELLI': 'IT', 'VITALE': 'IT', 'LOMBARDO': 'IT', 'SERRA': 'IT',
    'COPPOLA': 'IT', 'DEVITO': 'IT', 'DANGELO': 'IT', 'PALMIERI': 'IT', 'PELLEGRINO': 'IT',
    'ORLANDO': 'IT', 'PAGANO': 'IT', 'BENEDETTI': 'IT', 'VALENTINO': 'IT', 'ROSSETTI': 'IT',
    
    # Russian surnames - most common
    'IVANOV': 'RU', 'SMIRNOV': 'RU', 'KUZNETSOV': 'RU', 'POPOV': 'RU', 'VASILIEV': 'RU',
    'PETROV': 'RU', 'SOKOLOV': 'RU', 'MIKHAILOV': 'RU', 'NOVIKOV': 'RU', 'FEDOROV': 'RU',
    'MOROZOV': 'RU', 'VOLKOV': 'RU', 'ALEKSEEV': 'RU', 'LEBEDEV': 'RU', 'SEMENOV': 'RU',
    'EGOROV': 'RU', 'PAVLOV': 'RU', 'KOZLOV': 'RU', 'STEPANOV': 'RU', 'NIKOLAEV': 'RU',
    'ORLOV': 'RU', 'ANDREEV': 'RU', 'MAKAROV': 'RU', 'NIKITIN': 'RU', 'ZAKHAROV': 'RU',
    'ZAITSEV': 'RU', 'SOLOVYOV': 'RU', 'BORISOV': 'RU', 'YAKOVLEV': 'RU', 'GRIGORIEV': 'RU',
    'ROMANOV': 'RU', 'VOROBYOV': 'RU', 'KOVALEV': 'RU', 'BELOV': 'RU', 'KOMAROV': 'RU',
    'VINOGRADOV': 'RU', 'BOGDANOV': 'RU', 'MEDVEDEV': 'RU', 'ANTONOV': 'RU', 'MAXIMOV': 'RU',
    
    # Irish surnames - most common
    'MURPHY': 'IR', 'KELLY': 'IR', 'OSULLIVAN': 'IR', 'WALSH': 'IR', 'SMITH': 'IR',
    'OBRIEN': 'IR', 'BYRNE': 'IR', 'RYAN': 'IR', 'OCONNOR': 'IR', 'OFARRELL': 'IR',
    'MCCARTHY': 'IR', 'OREILLY': 'IR', 'DOYLE': 'IR', 'GALLAGHER': 'IR', 'ODOHERTY': 'IR',
    'KENNEDY': 'IR', 'LYNCH': 'IR', 'MURRAY': 'IR', 'QUINN': 'IR', 'MOORE': 'IR',
    'MCLAUGHLIN': 'IR', 'CARROLL': 'IR', 'CONNOLLY': 'IR', 'DALY': 'IR', 'CONNELL': 'IR',
    'SULLIVAN': 'IR', 'BRIEN': 'IR', 'DONOVAN': 'IR', 'MCCARTY': 'IR', 'MCCARTY': 'IR',
    'ONEILL': 'IR', 'REILLY': 'IR', 'DUFFY': 'IR', 'POWER': 'IR', 'BRENNAN': 'IR',
    'BURKE': 'IR', 'COLLINS': 'IR', 'CAMPBELL': 'IR', 'CLARKE': 'IR', 'JOHNSTON': 'IR',
    'HUGHES': 'IR', 'FARRELL': 'IR', 'FITZGERALD': 'IR', 'BROWN': 'IR', 'MARTIN': 'IR',
    'MCDONALD': 'IR', 'MCGUIRE': 'IR', 'MCMAHON': 'IR', 'MCKENNA': 'IR', 'MCDONAGH': 'IR',
    
    # Common African American surnames
    'WASHINGTON': 'B', 'JEFFERSON': 'B', 'BOOKER': 'B', 'BANKS': 'B',
    'MOSLEY': 'B', 'SINGLETON': 'B', 'BATTLE': 'B', 'FOUNTAIN': 'B',
}

# Ethnicity standardization mapping - Expanded with European Origins
ETHNICITY_STANDARDIZATION = {
    # White/Caucasian variants
    'WHITE': 'White',
    'CAUCASIAN': 'White',
    'WHITE / CAUCASIAN': 'White',
    'WHITE/CAUCASIAN': 'White',
    'W': 'White',
    'EUROPEAN': 'White',
    'EUROPEAN AMERICAN': 'White',
    
    # Italian variants
    'ITALIAN': 'Italian',
    'ITALIAN AMERICAN': 'Italian',
    'IT': 'Italian',
    
    # Irish variants
    'IRISH': 'Irish',
    'IRISH AMERICAN': 'Irish',
    'IR': 'Irish',
    
    # Russian variants
    'RUSSIAN': 'Russian',
    'RUSSIAN AMERICAN': 'Russian',
    'RU': 'Russian',
    
    # Black/African American variants
    'BLACK': 'Black',
    'AFRICAN AMERICAN': 'Black',
    'BLACK / AFRICAN AMERICAN': 'Black',
    'BLACK/AFRICAN AMERICAN': 'Black',
    'B': 'Black',
    'AFRICAN-AMERICAN': 'Black',
    'AA': 'Black',
    
    # Hispanic/Latino variants
    'HISPANIC': 'Hispanic',
    'LATINO': 'Hispanic',
    'LATINA': 'Hispanic',
    'HISPANIC OR LATINO': 'Hispanic',
    'HISPANIC/LATINO': 'Hispanic',
    'H': 'Hispanic',
    'LATINX': 'Hispanic',
    'MEXICAN': 'Hispanic',
    'PUERTO RICAN': 'Hispanic',
    'CUBAN': 'Hispanic',
    'SPANISH': 'Hispanic',
    
    # Chinese variants
    'CHINESE': 'Chinese',
    'CHINESE AMERICAN': 'Chinese',
    'CH': 'Chinese',
    
    # Asian/Pacific Islander variants (non-Chinese)
    'ASIAN': 'Asian',
    'ASIAN AMERICAN': 'Asian',
    'ASIAN / PACIFIC ISLANDER': 'Asian',
    'ASIAN/PACIFIC ISLANDER': 'Asian',
    'A': 'Asian',
    'PACIFIC ISLANDER': 'Asian',
    'JAPANESE': 'Asian',
    'KOREAN': 'Asian',
    'VIETNAMESE': 'Asian',
    'FILIPINO': 'Asian',
    'INDIAN': 'Asian',
    'SOUTH ASIAN': 'Asian',
    
    # Native American variants
    'NATIVE AMERICAN': 'Native American',
    'AMERICAN INDIAN': 'Native American',
    'INDIGENOUS': 'Native American',
    'NATIVE': 'Native American',
    'ALASKA NATIVE': 'Native American',
    'N': 'Native American',
    
    # Other/Multiple/Unknown
    'OTHER': 'Other',
    'MULTIPLE': 'Multiple',
    'TWO OR MORE RACES': 'Multiple',
    'MULTIRACIAL': 'Multiple',
    'MIXED': 'Multiple',
    'UNKNOWN': 'Unknown',
    'NOT PROVIDED': 'Unknown',
    'NO DATA PROVIDED': 'Unknown',
    'DECLINED': 'Unknown',
    'PREFER NOT TO SAY': 'Unknown',
    '': 'Unknown',
    'NULL': 'Unknown',
}


def standardize_ethnicity(raw_value: Optional[str]) -> str:
    """
    Standardize ethnicity value to consistent categories.
    
    Returns one of: White, Black, Hispanic, Asian, Chinese, Italian, Russian, Irish,
                    Native American, Multiple, Other, Unknown
    """
    if not raw_value:
        return 'Unknown'
    
    clean = raw_value.strip().upper()
    
    # Direct lookup
    if clean in ETHNICITY_STANDARDIZATION:
        return ETHNICITY_STANDARDIZATION[clean]
    
    # Partial matching - specific ethnicities first
    if 'ITALIAN' in clean:
        return 'Italian'
    if 'IRISH' in clean:
        return 'Irish'
    if 'RUSSIAN' in clean:
        return 'Russian'
    if 'CHINESE' in clean:
        return 'Chinese'
    
    # Then broad categories
    if 'WHITE' in clean or 'CAUCASIAN' in clean:
        return 'White'
    if 'BLACK' in clean or 'AFRICAN AMERICAN' in clean:
        return 'Black'
    if 'HISPANIC' in clean or 'LATINO' in clean or 'LATINA' in clean:
        return 'Hispanic'
    if 'ASIAN' in clean or 'PACIFIC' in clean:
        return 'Asian'
    if 'NATIVE' in clean or 'INDIGENOUS' in clean or 'INDIAN' in clean:
        return 'Native American'
    if 'MULTIPLE' in clean or 'TWO OR MORE' in clean or 'MIXED' in clean:
        return 'Multiple'
    
    return 'Other'


def predict_ethnicity_from_surname(last_name: Optional[str]) -> Optional[str]:
    """
    Predict ethnicity based on surname using expanded database.
    
    Returns: predicted ethnicity (Hispanic, Asian, Black, Chinese, Italian, Russian, Irish) or None
    """
    if not last_name:
        return None
    
    surname = last_name.strip().upper()
    
    # Direct lookup from database
    if surname in SURNAME_ETHNICITY_MAP:
        code = SURNAME_ETHNICITY_MAP[surname]
        if code == 'H':
            return 'Hispanic'
        elif code == 'A':
            return 'Asian'
        elif code == 'B':
            return 'Black'
        elif code == 'CH':
            return 'Chinese'
        elif code == 'IT':
            return 'Italian'
        elif code == 'RU':
            return 'Russian'
        elif code == 'IR':
            return 'Irish'
    
    # Pattern-based prediction for names not in database
    
    # Italian patterns (endings)
    if re.search(r'(INI|ELLI|ETTI|ACCI|UCCI|OLLI|AZZO|UZZI|ASSO)$', surname):
        return 'Italian'
    
    # Russian patterns (endings)
    if re.search(r'(OV|EV|IN|SKY|SKI|OVICH|EVICH)$', surname):
        return 'Russian'
    
    # Irish patterns (prefixes and common patterns)
    if re.search(r'^(O|MC|MAC)', surname):
        return 'Irish'
    
    # Hispanic patterns (Spanish/Portuguese endings)
    if re.search(r'(EZ|AZ|OS|AS)$', surname):
        hispanic_patterns = [
            r'GARCIA', r'RODRIGUEZ', r'MARTINEZ', r'HERNANDEZ', r'LOPEZ',
            r'GONZALEZ', r'PEREZ', r'SANCHEZ', r'RAMIREZ', r'FLORES',
        ]
        for pattern in hispanic_patterns:
            if re.search(pattern, surname):
                return 'Hispanic'
    
    # Chinese: Often 2-3 letters
    if len(surname) <= 3 and surname in ['LI', 'WU', 'HU', 'MA', 'XU', 'YE', 'GU']:
        return 'Chinese'
    
    # Vietnamese: specific patterns (falls under Asian, not Chinese)
    if surname in ['NGUYEN', 'TRAN', 'LE', 'PHAM', 'VU', 'DO', 'DANG', 'BUI', 'HOANG']:
        return 'Asian'
    
    # Korean: specific patterns (falls under Asian, not Chinese)
    if re.search(r'(KIM|PARK|LEE|CHOI|JUNG|KANG|CHO|YUN|SHIN|LIM)$', surname):
        return 'Asian'
    
    # Indian: specific patterns (falls under Asian, not Chinese)
    if re.search(r'(PATEL|KUMAR|SINGH|SHARMA|SHAH|MEHTA|GUPTA|REDDY|RAO)$', surname):
        return 'Asian'
    
    return None


def get_best_ethnicity(state_eth: Optional[str], 
                       modeled_eth: Optional[str], 
                       observed_eth: Optional[str],
                       last_name: Optional[str] = None) -> dict:
    """
    Determine the best ethnicity value using priority logic and prediction.
    
    Priority:
    1. StateEthnicity (if provided and not "No Data Provided")
    2. ObservedEthnicity (if provided and not "No Data Provided")
    3. ModeledEthnicity (if provided and not "No Data Provided")
    4. Surname prediction (if name provided)
    5. "Unknown"
    
    Returns dict with:
        - ethnicity: standardized ethnicity string
        - source: where the ethnicity came from (state/observed/modeled/predicted/unknown)
        - confidence: low/medium/high
    """
    # Priority 1: State ethnicity
    if state_eth and state_eth.strip().upper() not in ['NO DATA PROVIDED', 'UNKNOWN', '', 'NULL']:
        std = standardize_ethnicity(state_eth)
        if std != 'Unknown':
            return {
                'ethnicity': std,
                'source': 'state',
                'confidence': 'high'
            }
    
    # Priority 2: Observed ethnicity
    if observed_eth and observed_eth.strip().upper() not in ['NO DATA PROVIDED', 'UNKNOWN', '', 'NULL']:
        std = standardize_ethnicity(observed_eth)
        if std != 'Unknown':
            return {
                'ethnicity': std,
                'source': 'observed',
                'confidence': 'high'
            }
    
    # Priority 3: Modeled ethnicity
    if modeled_eth and modeled_eth.strip().upper() not in ['NO DATA PROVIDED', 'UNKNOWN', '', 'NULL']:
        std = standardize_ethnicity(modeled_eth)
        if std != 'Unknown':
            return {
                'ethnicity': std,
                'source': 'modeled',
                'confidence': 'medium'
            }
    
    # Priority 4: Surname prediction
    if last_name:
        predicted = predict_ethnicity_from_surname(last_name)
        if predicted:
            return {
                'ethnicity': predicted,
                'source': 'predicted',
                'confidence': 'low'
            }
    
    # Default: Unknown
    return {
        'ethnicity': 'Unknown',
        'source': 'none',
        'confidence': 'none'
    }


# SQL function for use in MySQL stored procedures
def generate_ethnicity_case_statement() -> str:
    """
    Generate SQL CASE statement for ethnicity standardization.
    Use this in MySQL queries.
    """
    cases = []
    
    for raw, standard in sorted(ETHNICITY_STANDARDIZATION.items()):
        if raw and raw != 'NULL':
            cases.append(f"WHEN UPPER(TRIM(eth_value)) = '{raw}' THEN '{standard}'")
    
    # Partial matches
    cases.append("WHEN UPPER(eth_value) LIKE '%WHITE%' OR UPPER(eth_value) LIKE '%CAUCASIAN%' THEN 'White'")
    cases.append("WHEN UPPER(eth_value) LIKE '%BLACK%' OR UPPER(eth_value) LIKE '%AFRICAN AMERICAN%' THEN 'Black'")
    cases.append("WHEN UPPER(eth_value) LIKE '%HISPANIC%' OR UPPER(eth_value) LIKE '%LATINO%' THEN 'Hispanic'")
    cases.append("WHEN UPPER(eth_value) LIKE '%ASIAN%' OR UPPER(eth_value) LIKE '%PACIFIC%' THEN 'Asian'")
    cases.append("WHEN UPPER(eth_value) LIKE '%NATIVE%' OR UPPER(eth_value) LIKE '%INDIGENOUS%' THEN 'Native American'")
    cases.append("WHEN UPPER(eth_value) LIKE '%MULTIPLE%' OR UPPER(eth_value) LIKE '%TWO OR MORE%' THEN 'Multiple'")
    
    sql = "CASE\n  " + "\n  ".join(cases) + "\n  ELSE 'Unknown'\nEND"
    return sql


if __name__ == "__main__":
    # Test cases
    test_cases = [
        ("White / Caucasian", None, None, "SMITH"),
        ("No Data Provided", "Black / African American", None, "JOHNSON"),
        ("No Data Provided", "No Data Provided", None, "GARCIA"),
        (None, None, None, "NGUYEN"),
        ("No Data Provided", "White / Caucasian", None, "PATEL"),
    ]
    
    print("Ethnicity Prediction Test Cases:")
    print("=" * 80)
    for state, modeled, observed, lastname in test_cases:
        result = get_best_ethnicity(state, modeled, observed, lastname)
        print(f"LastName: {lastname:15} State: {state or 'N/A':25}")
        print(f"  → Result: {result['ethnicity']:20} Source: {result['source']:10} Confidence: {result['confidence']}")
        print()
