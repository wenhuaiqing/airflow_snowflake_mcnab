import pandas as pd
import numpy as np
from faker import Faker
import random
import uuid
from datetime import datetime, timedelta

fake = Faker()

def generate_uuids(n):
    """
    Generate a list of UUIDs for primary keys.
    """
    return [str(uuid.uuid4()) for _ in range(n)]


def generate_constractors_data(num_contractors=50, date=None, filename="contractors.csv"):
    """
    Generate realistic contractor data for construction projects
    """
    contractor_types = ["General Contractor", "Electrical", "Plumbing", "HVAC", "Roofing", "Masonry", "Landscaping"]
    license_types = ["Class A", "Class B", "Class C", "Specialty", "Residential"]

    data = {
        "contractor_id": generate_uuids(num_contractors),
        "contractor_name": [fake.company() for _ in range(num_contractors)],
        "contractor_type": random.choices(contractor_types, k=num_contractors),
        "license_number": [f"LIC-{fake.random_number(digits=6)}" for _ in range(num_contractors)],
        "license_type": random.choices(license_types, k=num_contractors),
        "years_experience": np.random.randint(1, 30, size=num_contractors),
        "phone": [fake.phone_number() for _ in range(num_contractors)],
        "email": [fake.email() for _ in range(num_contractors)],
        "address": [fake.address() for _ in range(num_contractors)],
        "registration_date": [fake.date_between(start_date='-5y', end_date='-1y') for _ in range(num_contractors)],
        "update_at": date,
    }
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    return df


def generate_materials_data(filename="materials.csv", date=None):
    """
    Generate construction materials catalog with realistic types and prices
    """
    # Pre-defined UUIDs for consistent materials
    uuids = [
        "550e8400-e29b-41d4-a716-446655440000",
        "6fa459ea-ee8a-3ca4-894e-db77e160355e",
        "7d44a4d3-c8c8-45dd-bdc6-2222a56e7f5f",
        "123e4567-e89b-12d3-a456-426614174000",
        "e7fe1d3d-bd91-4f3d-8c26-e0cbe8ed3d6c",
        "34b34f28-afe0-4e78-80a1-8c9390c61dc6",
        "f47ac10b-58cc-4372-a567-0e02b2c3d479",
        "1e1cfbd8-1b1b-4b2c-9c3f-7f57c51925b1",
        "de305d54-75b4-431b-adb2-eb6b9e546014",
        "7b50f5c8-44e3-45a7-919e-dac4905b8d9c",
        "5a6b8baf-0f8d-4cc8-bf08-d85a89e8e5f3",
        "69e6b36c-3f72-4c4c-945c-1b0d9e8c2e9a",
        "b6f55d32-36db-4092-80e4-43b8b7b7c9ff",
        "348f1f5e-282d-4f07-ae7a-518a2b682dee",
        "7c7b0af2-9234-4c99-9b8b-7d0b4a9f7a58",
        "953b7c72-5e84-4d3e-b3e7-c6b24d9c59ea",
        "814b9d82-3769-45b3-a36e-5f3d7a5bbf7f",
        "92c1683a-1b84-4626-a807-afe73cdabc8b",
        "95c1063f-17d8-429b-97c2-3c7b59d7e89b",
        "816c5a91-1a24-4c26-8c3a-2b7b1bda73a5",
        "ee59c7f6-4a78-4cb9-82c7-4b84e7aebf4f",
        "4f0f81c7-7f54-4f07-8101-bb2c790c8c1b",
        "6311c573-40f7-47f9-9d2e-b74c2a4a8c8f",
        "4f63e6b9-4d1d-491d-bc5f-3e1b1f6f4e6d",
        "2a4df7d8-335c-4d8b-8d6c-7d43b4b5f1f9",
        "9b9f0c2f-0d94-4e4e-934c-b5f3a6a87e59",
        "ce47f814-6e25-4965-8f8c-97d487f8a7c7",
        "3b6d2c17-4129-4a56-8f9b-7b2d7e8f4e4b",
        "bc2e597a-4cf3-46b3-8f9a-61f7e7d9a2c8",
        "f6b89c3e-5e6f-487d-9e4b-2e9c7e8f4c6b",
    ]
    # Realistic construction materials
    material_names = [
        "Concrete Mix", "Steel Rebar", "Lumber 2x4", "Drywall Sheets", "Roofing Shingles",
        "Electrical Wire", "PVC Pipe", "Copper Pipe", "Insulation Batts", "Paint Gallon",
        "Cement Bags", "Brick", "Glass Panels", "Aluminum Siding", "Vinyl Flooring",
        "Ceramic Tile", "Granite Countertop", "Kitchen Cabinets", "Bathroom Fixtures",
        "Lighting Fixtures", "HVAC Ductwork", "Plumbing Fixtures", "Windows", "Doors",
        "Staircase", "Decking Material", "Fencing", "Landscaping Rock", "Mulch", "Sealant"
    ]
    material_categories = [
        "Foundation", "Structural", "Framing", "Interior", "Roofing",
        "Electrical", "Plumbing", "Insulation", "Finishing", "Foundation",
        "Masonry", "Glazing", "Exterior", "Flooring", "Tile",
        "Countertops", "Cabinetry", "Fixtures", "Lighting", "HVAC",
        "Plumbing", "Windows", "Doors", "Stairs", "Exterior",
        "Fencing", "Landscaping", "Landscaping", "Finishing", "Sealants"
    ]
    prices = np.round(np.random.uniform(5.0, 5000.0, size=len(material_names)), 2)

    data = {
        "material_id": uuids,
        "material_name": material_names,
        "material_category": material_categories,
        "unit_price": prices,
        "unit": random.choices(["piece", "sqft", "linear_ft", "cubic_yd", "gallon", "bag"], k=len(material_names)),
        "update_at": date,
    }
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    return df


def generate_projects_data(num_projects=100, date=None, filename="projects.csv"):
    """
    Generate construction projects data
    """
    project_types = ["Residential", "Commercial", "Industrial", "Infrastructure", "Renovation"]
    project_statuses = ["Planning", "In Progress", "On Hold", "Completed", "Cancelled"]

    data = {
        "project_id": generate_uuids(num_projects),
        "project_name": [f"{fake.word().title()} {fake.word().title()} Project" for _ in range(num_projects)],
        "project_type": random.choices(project_types, k=num_projects),
        "project_status": random.choices(project_statuses, k=num_projects),
        "start_date": [fake.date_between(start_date='-2y', end_date='+6m') for _ in range(num_projects)],
        "estimated_completion": [fake.date_between(start_date='-1y', end_date='+2y') for _ in range(num_projects)],
        "budget": np.round(np.random.uniform(50000, 5000000, size=num_projects), 2),
        "square_feet": np.random.randint(1000, 50000, size=num_projects),
        "location": [fake.city() for _ in range(num_projects)],
        "client_name": [fake.company() for _ in range(num_projects)],
        "architect": [fake.name() for _ in range(num_projects)],
        "update_at": date,
    }
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    return df


def generate_project_activities_data(num_activities=500, projects_df=None, contractors_df=None, materials_df=None, date=None, filename="project_activities.csv"):
    """
    Generate project activities data linking projects, contractors, and materials
    """
    if projects_df is None or contractors_df is None or materials_df is None:
        raise ValueError("Projects, Contractors, and Materials dataframes must be provided")

    activity_types = ["Foundation", "Framing", "Electrical", "Plumbing", "HVAC", "Roofing", "Interior", "Exterior", "Landscaping", "Inspection"]

    if num_activities == 1:
        data = {
            "activity_id": generate_uuids(1),
            "project_id": projects_df["project_id"].iloc[0],
            "contractor_id": contractors_df['contractor_id'].iloc[0],
            "material_id": materials_df['material_id'].iloc[0],
            "activity_type": random.choice(activity_types),
            "quantity": np.random.randint(1, 100),
            "activity_date": date,
            "hours_worked": np.random.randint(1, 12),
            "cost": np.random.randint(100, 10000),
        }
    else:
        data = {
            "activity_id": generate_uuids(num_activities),
            "project_id": projects_df["project_id"].sample(n=num_activities, replace=True).values,
            "contractor_id": contractors_df['contractor_id'].sample(n=num_activities, replace=True).values,
            "material_id": materials_df['material_id'].sample(n=num_activities, replace=True).values,
            "activity_type": random.choices(activity_types, k=num_activities),
            "quantity": np.random.randint(1, 100, size=num_activities),
            "activity_date": date,
            "hours_worked": np.random.randint(1, 12, size=num_activities),
            "cost": np.random.randint(100, 10000, size=num_activities),
        }
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    return df


def get_new_construction_data_from_internal_api(num_activities, date):
    """
    Main function that simulates calling an internal API to get new construction data
    """
        # Generate proportional data
    num_projects = int(0.2 * num_activities)  # 20% of activities
    if num_projects < 1:
        num_projects = 1
    
    num_contractors = int(0.3 * num_activities)  # 30% of activities
    if num_contractors < 1:
        num_contractors = 1
    
    # Generate all related data
    projects_df = generate_projects_data(num_projects=num_projects, date=date)
    contractors_df = generate_contractors_data(num_contractors=num_contractors, date=date)
    materials_df = generate_materials_data(date=date)
    activities_df = generate_project_activities_data(
        num_activities=num_activities, 
        projects_df=projects_df, 
        contractors_df=contractors_df, 
        materials_df=materials_df, 
        date=date
    )
    return activities_df, projects_df, contractors_df, materials_df
