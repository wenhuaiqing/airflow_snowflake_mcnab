from datetime import datetime, timedelta


column_mapping_users = {
    "DATE_OF_BIRTH": {
        "min": {"geq_to": datetime.strptime("1900-01-01", "%Y-%m-%d").date()},
        # Users need to be at least 13 years old
        "max": {"leq_to": (datetime.now() - timedelta(days=13 * 365)).date()},
    },
    "SIGN_UP_DATE": {
        "min": {"geq_to": datetime.strptime("2020-01-01", "%Y-%m-%d").date()},
        "max": {"leq_to": datetime.now().date()},
    },
}

column_mapping_utms = {
    "UTM_SOURCE": {
        "null_check": {"equal_to": 0},
    },
    "UTM_MEDIUM": {
        "null_check": {"equal_to": 0},
    },
    "UTM_CAMPAIGN": {
        "null_check": {"equal_to": 0},
    },
}

column_mapping_sales = {
    "QUANTITY": {
        "null_check": {"equal_to": 0},
        "min": {"geq_to": 0},
        "max": {"leq_to": 100},
    }
}

# Data quality checks configuration for construction data tables

column_mappings = {
    "contractors": {
        "CONTRACTOR_ID": {
            "unique_check": {"equal_to": 0},
            "null_check": {"equal_to": 0},
        },
        "CONTRACTOR_NAME": {
            "null_check": {"equal_to": 0},
        },
        "CONTRACTOR_TYPE": {
            "null_check": {"equal_to": 0},
        },
        "LICENSE_NUMBER": {
            "null_check": {"equal_to": 0},
        },
        "YEARS_EXPERIENCE": {
            "null_check": {"equal_to": 0},
            "min": {"geq_to": 0},
            "max": {"leq_to": 50},
        },
    },
    "materials": {
        "MATERIAL_ID": {
            "unique_check": {"equal_to": 0},
            "null_check": {"equal_to": 0},
        },
        "MATERIAL_NAME": {
            "null_check": {"equal_to": 0},
        },
        "MATERIAL_CATEGORY": {
            "null_check": {"equal_to": 0},
        },
        "UNIT_PRICE": {
            "null_check": {"equal_to": 0},
            "min": {"geq_to": 0},
            "max": {"leq_to": 10000},
        },
    },
    "projects": {
        "PROJECT_ID": {
            "unique_check": {"equal_to": 0},
            "null_check": {"equal_to": 0},
        },
        "PROJECT_NAME": {
            "null_check": {"equal_to": 0},
        },
        "PROJECT_TYPE": {
            "null_check": {"equal_to": 0},
        },
        "BUDGET": {
            "null_check": {"equal_to": 0},
            "min": {"geq_to": 1000},
            "max": {"leq_to": 10000000},
        },
        "SQUARE_FEET": {
            "null_check": {"equal_to": 0},
            "min": {"geq_to": 100},
            "max": {"leq_to": 100000},
        },
    },
    "project_activities": {
        "ACTIVITY_ID": {
            "unique_check": {"equal_to": 0},
            "null_check": {"equal_to": 0},
        },
        "PROJECT_ID": {
            "null_check": {"equal_to": 0},
        },
        "CONTRACTOR_ID": {
            "null_check": {"equal_to": 0},
        },
        "MATERIAL_ID": {
            "null_check": {"equal_to": 0},
        },
        "QUANTITY": {
            "null_check": {"equal_to": 0},
            "min": {"geq_to": 1},
            "max": {"leq_to": 1000},
        },
        "HOURS_WORKED": {
            "null_check": {"equal_to": 0},
            "min": {"geq_to": 1},
            "max": {"leq_to": 24},
        },
        "COST": {
            "null_check": {"equal_to": 0},
            "min": {"geq_to": 0},
            "max": {"leq_to": 100000},
        },
    },
}

