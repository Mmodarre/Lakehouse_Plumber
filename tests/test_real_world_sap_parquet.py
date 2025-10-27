"""Real-world test using actual SAP parquet ingestion files.

This test demonstrates the practical benefit of multi-flowgroup support by combining
3 nearly-identical SAP master data ingestion files into a single file, reducing
file proliferation while maintaining identical functionality.
"""

import pytest
import tempfile
from pathlib import Path

from lhp.parsers.yaml_parser import YAMLParser
from lhp.core.services.flowgroup_discoverer import FlowgroupDiscoverer


class TestSAPParquetMultiFlowgroup:
    """Real-world test with SAP parquet ingestion flowgroups."""
    
    def test_multi_document_syntax_sap_files(self):
        """Test combining 3 SAP parquet files into one multi-document file."""
        parser = YAMLParser()
        
        # Create combined multi-document file (based on actual SAP files)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""# Brand master data ingestion from SAP
pipeline: raw_ingestions_sap
flowgroup: sap_brand_ingestion_TMPL003
use_template: TMPL003_parquet_ingestion_template
template_parameters:
  table_name: raw_sap_brand
  landing_folder: brand
---
# Category master data ingestion from SAP
pipeline: raw_ingestions_sap
flowgroup: sap_cat_ingestion_TMPL003
use_template: TMPL003_parquet_ingestion_template
template_parameters:
  table_name: raw_sap_cat
  landing_folder: category
---
# Carrier master data ingestion from SAP
pipeline: raw_ingestions_sap
flowgroup: sap_carrier_ingestion_TMPL003
use_template: TMPL003_parquet_ingestion_template
template_parameters:
  table_name: raw_sap_carrier
  landing_folder: carrier
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            flowgroups = parser.parse_flowgroups_from_file(yaml_file)
            
            # Verify 3 flowgroups discovered
            assert len(flowgroups) == 3
            
            # Verify flowgroup names
            flowgroup_names = [fg.flowgroup for fg in flowgroups]
            assert flowgroup_names == [
                'sap_brand_ingestion_TMPL003',
                'sap_cat_ingestion_TMPL003',
                'sap_carrier_ingestion_TMPL003'
            ]
            
            # Verify all use same pipeline
            assert all(fg.pipeline == 'raw_ingestions_sap' for fg in flowgroups)
            
            # Verify all use same template
            assert all(fg.use_template == 'TMPL003_parquet_ingestion_template' for fg in flowgroups)
            
            # Verify template parameters are unique
            assert flowgroups[0].template_parameters['table_name'] == 'raw_sap_brand'
            assert flowgroups[0].template_parameters['landing_folder'] == 'brand'
            assert flowgroups[1].template_parameters['table_name'] == 'raw_sap_cat'
            assert flowgroups[1].template_parameters['landing_folder'] == 'category'
            assert flowgroups[2].template_parameters['table_name'] == 'raw_sap_carrier'
            assert flowgroups[2].template_parameters['landing_folder'] == 'carrier'
            
        finally:
            yaml_file.unlink()
    
    def test_array_syntax_sap_files(self):
        """Test combining 3 SAP parquet files using array syntax with inheritance."""
        parser = YAMLParser()
        
        # Create combined array syntax file with shared configuration
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""# SAP Master Data Ingestions
pipeline: raw_ingestions_sap
use_template: TMPL003_parquet_ingestion_template
flowgroups:
  - flowgroup: sap_brand_ingestion_TMPL003
    template_parameters:
      table_name: raw_sap_brand
      landing_folder: brand
  - flowgroup: sap_cat_ingestion_TMPL003
    template_parameters:
      table_name: raw_sap_cat
      landing_folder: category
  - flowgroup: sap_carrier_ingestion_TMPL003
    template_parameters:
      table_name: raw_sap_carrier
      landing_folder: carrier
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            flowgroups = parser.parse_flowgroups_from_file(yaml_file)
            
            # Verify 3 flowgroups discovered
            assert len(flowgroups) == 3
            
            # Verify inheritance worked - all should have inherited use_template and pipeline
            assert all(fg.pipeline == 'raw_ingestions_sap' for fg in flowgroups)
            assert all(fg.use_template == 'TMPL003_parquet_ingestion_template' for fg in flowgroups)
            
            # Verify template parameters are unique (not inherited)
            assert flowgroups[0].template_parameters['table_name'] == 'raw_sap_brand'
            assert flowgroups[1].template_parameters['table_name'] == 'raw_sap_cat'
            assert flowgroups[2].template_parameters['table_name'] == 'raw_sap_carrier'
            
        finally:
            yaml_file.unlink()
    
    def test_discoverer_with_multi_flowgroup_sap_file(self):
        """Test FlowgroupDiscoverer with combined SAP file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            pipelines_dir = project_root / "pipelines" / "01_raw_ingestion" / "SAP"
            pipelines_dir.mkdir(parents=True)
            
            # Create combined file
            (pipelines_dir / "sap_master_data.yaml").write_text("""
pipeline: raw_ingestions_sap
use_template: TMPL003_parquet_ingestion_template
flowgroups:
  - flowgroup: sap_brand_ingestion_TMPL003
    template_parameters:
      table_name: raw_sap_brand
      landing_folder: brand
  - flowgroup: sap_cat_ingestion_TMPL003
    template_parameters:
      table_name: raw_sap_cat
      landing_folder: category
  - flowgroup: sap_carrier_ingestion_TMPL003
    template_parameters:
      table_name: raw_sap_carrier
      landing_folder: carrier
""")
            
            discoverer = FlowgroupDiscoverer(project_root)
            flowgroups = discoverer.discover_flowgroups(pipelines_dir)
            
            # Should discover all 3 from single file
            assert len(flowgroups) == 3
            flowgroup_names = {fg.flowgroup for fg in flowgroups}
            assert flowgroup_names == {
                'sap_brand_ingestion_TMPL003',
                'sap_cat_ingestion_TMPL003',
                'sap_carrier_ingestion_TMPL003'
            }
    
    def test_file_reduction_comparison(self):
        """Demonstrate file reduction: 3 files → 1 file with same functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            pipelines_dir = project_root / "pipelines" / "test"
            pipelines_dir.mkdir(parents=True)
            
            # Scenario A: 3 separate files (old approach)
            (pipelines_dir / "brand.yaml").write_text("""
pipeline: raw_ingestions_sap
flowgroup: sap_brand_ingestion_TMPL003
use_template: TMPL003_parquet_ingestion_template
template_parameters:
  table_name: raw_sap_brand
  landing_folder: brand
""")
            
            (pipelines_dir / "cat.yaml").write_text("""
pipeline: raw_ingestions_sap
flowgroup: sap_cat_ingestion_TMPL003
use_template: TMPL003_parquet_ingestion_template
template_parameters:
  table_name: raw_sap_cat
  landing_folder: category
""")
            
            (pipelines_dir / "carrier.yaml").write_text("""
pipeline: raw_ingestions_sap
flowgroup: sap_carrier_ingestion_TMPL003
use_template: TMPL003_parquet_ingestion_template
template_parameters:
  table_name: raw_sap_carrier
  landing_folder: carrier
""")
            
            # Discover with 3 separate files
            discoverer = FlowgroupDiscoverer(project_root)
            flowgroups_separate = discoverer.discover_flowgroups(pipelines_dir)
            
            # Clean up for scenario B
            for f in pipelines_dir.glob("*.yaml"):
                f.unlink()
            
            # Scenario B: 1 combined file (new approach)
            (pipelines_dir / "sap_master_data.yaml").write_text("""
pipeline: raw_ingestions_sap
use_template: TMPL003_parquet_ingestion_template
flowgroups:
  - flowgroup: sap_brand_ingestion_TMPL003
    template_parameters:
      table_name: raw_sap_brand
      landing_folder: brand
  - flowgroup: sap_cat_ingestion_TMPL003
    template_parameters:
      table_name: raw_sap_cat
      landing_folder: category
  - flowgroup: sap_carrier_ingestion_TMPL003
    template_parameters:
      table_name: raw_sap_carrier
      landing_folder: carrier
""")
            
            # Discover with 1 combined file
            flowgroups_combined = discoverer.discover_flowgroups(pipelines_dir)
            
            # Same number of flowgroups discovered
            assert len(flowgroups_separate) == len(flowgroups_combined) == 3
            
            # Same flowgroup names
            names_separate = {fg.flowgroup for fg in flowgroups_separate}
            names_combined = {fg.flowgroup for fg in flowgroups_combined}
            assert names_separate == names_combined
            
            # File count reduced: 3 → 1 (67% reduction)
            assert len(list(pipelines_dir.glob("*.yaml"))) == 1

