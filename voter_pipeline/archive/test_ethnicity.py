#!/usr/bin/env python3
"""
Test Ethnicity Prediction Module
Validates standardization and surname prediction logic
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from ethnicity_predictor import (
    standardize_ethnicity,
    predict_ethnicity_from_surname,
    get_best_ethnicity,
    SURNAME_ETHNICITY_MAP
)


def test_standardization():
    """Test ethnicity value standardization"""
    print("=" * 80)
    print("TEST 1: Ethnicity Standardization")
    print("=" * 80)
    
    test_cases = [
        ("White / Caucasian", "White"),
        ("BLACK / AFRICAN AMERICAN", "Black"),
        ("Hispanic or Latino", "Hispanic"),
        ("Asian / Pacific Islander", "Asian"),
        ("Native American", "Native American"),
        ("No Data Provided", "Unknown"),
        ("", "Unknown"),
        (None, "Unknown"),
        ("Two or More Races", "Multiple"),
        ("W", "White"),
        ("B", "Black"),
        ("H", "Hispanic"),
        ("A", "Asian"),
        ("Italian", "Italian"),
        ("Italian American", "Italian"),
        ("Irish", "Irish"),
        ("Russian", "Russian"),
        ("Chinese", "Chinese"),
    ]
    
    passed = 0
    failed = 0
    
    for input_val, expected in test_cases:
        result = standardize_ethnicity(input_val)
        status = "[PASS]" if result == expected else "[FAIL]"
        if result == expected:
            passed += 1
        else:
            failed += 1
        print(f"{status} Input: {str(input_val):30} -> Expected: {expected:20} Got: {result}")
    
    print(f"\nResults: {passed} passed, {failed} failed\n")
    return failed == 0


def test_surname_prediction():
    """Test surname-based ethnicity prediction"""
    print("=" * 80)
    print("TEST 2: Surname Prediction")
    print("=" * 80)
    
    test_cases = [
        ("GARCIA", "Hispanic"),
        ("RODRIGUEZ", "Hispanic"),
        ("NGUYEN", "Asian"),
        ("WANG", "Chinese"),
        ("PATEL", "Asian"),
        ("KIM", "Asian"),
        ("WASHINGTON", "Black"),
        ("RUSSO", "Italian"),
        ("FERRARI", "Italian"),
        ("MURPHY", "Irish"),
        ("KELLY", "Irish"),
        ("IVANOV", "Russian"),
        ("PETROV", "Russian"),
        ("SMITH", "Irish"),  # SMITH is in our Irish database
        ("JOHNSON", None),   # No prediction for this common name
        ("", None),
        (None, None),
    ]
    
    passed = 0
    failed = 0
    
    for surname, expected in test_cases:
        result = predict_ethnicity_from_surname(surname)
        status = "[PASS]" if result == expected else "[FAIL]"
        if result == expected:
            passed += 1
        else:
            failed += 1
        print(f"{status} Surname: {str(surname):20} -> Expected: {str(expected):20} Got: {str(result)}")
    
    print(f"\nResults: {passed} passed, {failed} failed\n")
    return failed == 0


def test_priority_logic():
    """Test get_best_ethnicity priority logic"""
    print("=" * 80)
    print("TEST 3: Priority Logic (State > Observed > Modeled > Predicted > Unknown)")
    print("=" * 80)
    
    test_cases = [
        {
            'state': 'White / Caucasian',
            'modeled': 'Black / African American',
            'observed': 'Hispanic',
            'lastname': 'GARCIA',
            'expected': ('White', 'state', 'high')
        },
        {
            'state': 'No Data Provided',
            'modeled': 'Black / African American',
            'observed': 'Hispanic',
            'lastname': 'GARCIA',
            'expected': ('Hispanic', 'observed', 'high')
        },
        {
            'state': 'No Data Provided',
            'modeled': 'Black / African American',
            'observed': 'No Data Provided',
            'lastname': 'GARCIA',
            'expected': ('Black', 'modeled', 'medium')
        },
        {
            'state': 'No Data Provided',
            'modeled': 'No Data Provided',
            'observed': 'No Data Provided',
            'lastname': 'NGUYEN',
            'expected': ('Asian', 'predicted', 'low')
        },
        {
            'state': None,
            'modeled': None,
            'observed': None,
            'lastname': 'JOHNSON',  # Changed from SMITH (which is Irish in our DB)
            'expected': ('Unknown', 'none', 'none')
        },
    ]
    
    passed = 0
    failed = 0
    
    for i, test in enumerate(test_cases, 1):
        result = get_best_ethnicity(
            test['state'],
            test['modeled'],
            test['observed'],
            test['lastname']
        )
        
        expected_eth, expected_src, expected_conf = test['expected']
        is_correct = (
            result['ethnicity'] == expected_eth and
            result['source'] == expected_src and
            result['confidence'] == expected_conf
        )
        
        status = "[PASS]" if is_correct else "[FAIL]"
        if is_correct:
            passed += 1
        else:
            failed += 1
        
        print(f"{status} Test Case {i}:")
        print(f"   State: {test['state']}")
        print(f"   Observed: {test['observed']}")
        print(f"   Modeled: {test['modeled']}")
        print(f"   LastName: {test['lastname']}")
        print(f"   Expected: {expected_eth} (source: {expected_src}, confidence: {expected_conf})")
        print(f"   Got:      {result['ethnicity']} (source: {result['source']}, confidence: {result['confidence']})")
        print()
    
    print(f"Results: {passed} passed, {failed} failed\n")
    return failed == 0


def test_surname_coverage():
    """Display surname coverage statistics"""
    print("=" * 80)
    print("TEST 4: Surname Database Coverage")
    print("=" * 80)
    
    print(f"Total surnames in database: {len(SURNAME_ETHNICITY_MAP)}")
    
    by_ethnicity = {}
    for surname, code in SURNAME_ETHNICITY_MAP.items():
        if code == 'H':
            eth = 'Hispanic'
        elif code == 'A':
            eth = 'Asian (non-Chinese)'
        elif code == 'B':
            eth = 'Black'
        elif code == 'CH':
            eth = 'Chinese'
        elif code == 'IT':
            eth = 'Italian'
        elif code == 'RU':
            eth = 'Russian'
        elif code == 'IR':
            eth = 'Irish'
        else:
            eth = 'Other'
        
        by_ethnicity[eth] = by_ethnicity.get(eth, 0) + 1
    
    print("\nBreakdown by ethnicity:")
    for eth, count in sorted(by_ethnicity.items(), key=lambda x: x[1], reverse=True):
        print(f"  {eth:25}: {count:4} surnames")
    
    print("\nTop 10 Hispanic surnames:")
    hispanic = [s for s, c in SURNAME_ETHNICITY_MAP.items() if c == 'H'][:10]
    print("  " + ", ".join(hispanic))
    
    print("\nTop 10 Chinese surnames:")
    chinese = [s for s, c in SURNAME_ETHNICITY_MAP.items() if c == 'CH'][:10]
    print("  " + ", ".join(chinese))
    
    print("\nTop 10 Italian surnames:")
    italian = [s for s, c in SURNAME_ETHNICITY_MAP.items() if c == 'IT'][:10]
    print("  " + ", ".join(italian))
    
    print("\nTop 10 Irish surnames:")
    irish = [s for s, c in SURNAME_ETHNICITY_MAP.items() if c == 'IR'][:10]
    print("  " + ", ".join(irish))
    
    print("\nTop 10 Russian surnames:")
    russian = [s for s, c in SURNAME_ETHNICITY_MAP.items() if c == 'RU'][:10]
    print("  " + ", ".join(russian))
    
    print()
    return True


def main():
    """Run all tests"""
    print("\n")
    print("=" * 80)
    print("  ETHNICITY PREDICTOR TEST SUITE")
    print("=" * 80)
    print()
    
    all_passed = True
    
    all_passed &= test_standardization()
    all_passed &= test_surname_prediction()
    all_passed &= test_priority_logic()
    all_passed &= test_surname_coverage()
    
    print("=" * 80)
    if all_passed:
        print("[PASS] ALL TESTS PASSED")
    else:
        print("[FAIL] SOME TESTS FAILED")
    print("=" * 80)
    print()
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
