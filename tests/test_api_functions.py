"""
Unit tests for API functions
Run with: pytest tests/test_api_functions.py
"""

import pytest
import pandas as pd
from include.api_functions import (
    get_new_construction_data_from_internal_api,
    generate_contractors_data,
    generate_materials_data,
    generate_projects_data,
    generate_project_activities_data
)


class TestAPIFunctions:
    """Test class for API functions"""
    
    def test_generate_contractors_data(self):
        """Test contractor data generation"""
        df = generate_contractors_data(num_contractors=5, date="2024-01-15")
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert "contractor_id" in df.columns
        assert "contractor_name" in df.columns
        assert "contractor_type" in df.columns
        
        # Check data types
        assert df["contractor_id"].dtype == "object"
        assert df["years_experience"].dtype == "int64"
    
    def test_generate_materials_data(self):
        """Test materials data generation"""
        df = generate_materials_data(date="2024-01-15")
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 30  # 30 materials
        assert "material_id" in df.columns
        assert "material_name" in df.columns
        assert "unit_price" in df.columns
        
        # Check price range
        assert df["unit_price"].min() >= 5.0
        assert df["unit_price"].max() <= 5000.0
    
    def test_generate_projects_data(self):
        """Test projects data generation"""
        df = generate_projects_data(num_projects=10, date="2024-01-15")
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10
        assert "project_id" in df.columns
        assert "project_name" in df.columns
        assert "budget" in df.columns
        
        # Check budget range
        assert df["budget"].min() >= 50000
        assert df["budget"].max() <= 5000000
    
    def test_generate_project_activities_data(self):
        """Test project activities data generation"""
        # Create sample data for testing
        projects_df = generate_projects_data(num_projects=2, date="2024-01-15")
        contractors_df = generate_contractors_data(num_contractors=2, date="2024-01-15")
        materials_df = generate_materials_data(date="2024-01-15")
        
        df = generate_project_activities_data(
            num_activities=5,
            projects_df=projects_df,
            contractors_df=contractors_df,
            materials_df=materials_df,
            date="2024-01-15"
        )
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert "activity_id" in df.columns
        assert "project_id" in df.columns
        assert "contractor_id" in df.columns
        assert "material_id" in df.columns
        assert "cost" in df.columns
    
    def test_main_api_function(self):
        """Test the main API function"""
        activities_df, projects_df, contractors_df, materials_df = get_new_construction_data_from_internal_api(
            num_activities=10, 
            date="2024-01-15"
        )
        
        # Check all dataframes are returned
        assert isinstance(activities_df, pd.DataFrame)
        assert isinstance(projects_df, pd.DataFrame)
        assert isinstance(contractors_df, pd.DataFrame)
        assert isinstance(materials_df, pd.DataFrame)
        
        # Check data relationships
        assert len(activities_df) == 10
        assert len(projects_df) >= 1  # At least 1 project
        assert len(contractors_df) >= 1  # At least 1 contractor
        assert len(materials_df) == 30  # Always 30 materials
        
        # Check foreign key relationships exist
        project_ids = set(projects_df["project_id"])
        contractor_ids = set(contractors_df["contractor_id"])
        material_ids = set(materials_df["material_id"])
        
        # All activities should reference existing projects, contractors, and materials
        for project_id in activities_df["project_id"]:
            assert project_id in project_ids
        
        for contractor_id in activities_df["contractor_id"]:
            assert contractor_id in contractor_ids
        
        for material_id in activities_df["material_id"]:
            assert material_id in material_ids


if __name__ == "__main__":
    # Run tests manually
    pytest.main([__file__, "-v"])
