import shutil
import re
import logging
from pathlib import Path
from typing import Any, List, Dict
from jinja2 import Environment, FileSystemLoader
from geppetto.data.models.data_source import DataSourceConfig
from geppetto.data.models.rule import DiscrepancyRule


logger = logging.getLogger(__name__)

# Common import to package mapping
IMPORT_TO_PACKAGE = {
    "polars": "polars>=0.20.0",
    "pl": "polars>=0.20.0",
    "pandas": "pandas>=2.0.0",
    "pd": "pandas>=2.0.0",
    "numpy": "numpy>=1.24.0",
    "np": "numpy>=1.24.0",
    "datetime": None,  # stdlib
    "json": None,  # stdlib
    "re": None,  # stdlib
    "math": None,  # stdlib
    "typing": None,  # stdlib
    "collections": None,  # stdlib
    "itertools": None,  # stdlib
    "functools": None,  # stdlib
    "requests": "requests>=2.31.0",
    "httpx": "httpx>=0.25.0",
    "sqlalchemy": "sqlalchemy>=2.0.0",
    "psycopg2": "psycopg2-binary>=2.9.0",
    "pymysql": "pymysql>=1.1.0",
    "redis": "redis>=5.0.0",
    "geopy": "geopy>=2.4.0",
    "shapely": "shapely>=2.0.0",
}


class CodeSynthesizer:
    def __init__(self, template_dir: str = "templates/child_app"):
        self.template_dir = Path(template_dir)
        # Auto-reload disabled for production performance, enabled for debugging
        self.env = Environment(
            loader=FileSystemLoader(self.template_dir), keep_trailing_newline=True
        )

    def prepare_detectors_context(self, rules: List[DiscrepancyRule]) -> List[Dict]:
        """
        Prepares the list of function definitions for the Jinja template.
        """
        detector_contexts = []
        for rule in rules:
            # Extract function name from code
            func_name = f"check_{rule.rule_id}"
            if rule.code:
                match = re.search(r"def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(", rule.code)
                if match:
                    func_name = match.group(1)

            # We now use the pre-generated code directly
            # The template expects 'code' which is the full function definition
            detector_contexts.append(
                {
                    "func_name": func_name,
                    "rule_id": rule.rule_id,
                    "description": rule.description,
                    "severity": rule.severity.value,
                    "code": rule.code,  # Injected directly
                    "explanation": rule.explanation,
                }
            )
        return detector_contexts

    def prepare_config_context(
        self, data_source_config: DataSourceConfig
    ) -> Dict[str, Any]:
        """
        Prepares data source configuration for template rendering.
        Extracts relevant fields based on the data source type.
        """
        # Convert Pydantic model to dict for easier access
        config_dict = data_source_config.model_dump()
        config_type = config_dict.get("type", "manual")

        context = {
            "data_source_type": config_type,
            "connection_string": config_dict.get("connection_string", ""),
            "query": config_dict.get("query", ""),
            "file_path": config_dict.get("file_path", ""),
            "api_url": config_dict.get("api_url", ""),
            "api_page_size": config_dict.get("api_page_size", 100),
            "batch_size": config_dict.get("batch_size", 1000),
            "headers": config_dict.get("headers", {}),
            "auth_token": config_dict.get("auth_token", ""),
            "start_date_column": config_dict.get("start_date_column", ""),
            "end_date_column": config_dict.get("end_date_column", ""),
        }

        return context

    def extract_dependencies(self, rules: List[DiscrepancyRule]) -> List[str]:
        """
        Extracts Python package dependencies from detector code.
        Returns a list of package specifications (e.g., ['pandas>=2.0.0']).
        """
        # Base dependencies always needed for the script
        base_deps = {
            "polars>=0.20.0",
            "connectorx>=0.3.2",
            "httpx>=0.27.0",
            "pydantic>=2.0.0",
            "pydantic-settings>=2.0.0",
            "python-dotenv>=1.0.0",
        }

        detected_packages = set()

        for rule in rules:
            if not rule.code:
                continue

            # Extract import statements
            import_pattern = (
                r"^\s*(?:from\s+([\w.]+)\s+import|import\s+([\w.]+(?:\s*,\s*[\w.]+)*))"
            )

            for line in rule.code.split("\n"):
                match = re.match(import_pattern, line)
                if match:
                    # Handle 'from X import' or 'import X'
                    module = match.group(1) or match.group(2)
                    if module:
                        # Get the top-level module name
                        top_level = module.split(".")[0].strip()

                        # Map to package if known
                        if top_level in IMPORT_TO_PACKAGE:
                            package = IMPORT_TO_PACKAGE[top_level]
                            if package:  # Skip stdlib modules (None)
                                detected_packages.add(package)

            # Add explicit dependencies from the rule model
            if rule.dependencies:
                for dep in rule.dependencies:
                    detected_packages.add(dep)

        # Combine base and detected dependencies
        all_deps = base_deps | detected_packages
        logger.debug(
            f"Extracted {len(all_deps)} total dependencies ({len(detected_packages)} detected)"
        )
        return sorted(list(all_deps))

    def prepare_dependencies_context(
        self, rules: List[DiscrepancyRule]
    ) -> Dict[str, Any]:
        """
        Prepares dependencies context for pyproject.toml template.
        """
        dependencies = self.extract_dependencies(rules)
        return {"dependencies": dependencies}

    def generate_codebase(
        self,
        project_id: str,
        rule_set: List[DiscrepancyRule],
        data_source_config: DataSourceConfig,
        output_dir: Path,
    ):
        """
        Main entry point. Renders all templates and writes to output_dir.

        Args:
            rule_set: The validated rule set containing detection logic
            data_source_config: Configuration for the data source
            output_dir: Directory where generated code will be written
        """
        logger.info(
            f"Starting code generation for project {project_id} with {len(rule_set)} rules"
        )
        # Preserve .git directory if it exists (for deployment workflow)
        git_dir = output_dir / ".git"
        git_backup = None

        if git_dir.exists():
            # Temporarily move .git to preserve it
            git_backup = output_dir.parent / f".git_backup_{project_id}"
            shutil.move(str(git_dir), str(git_backup))

        # Clean and recreate output directory
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True)

        # Restore .git directory if it was backed up
        if git_backup and git_backup.exists():
            shutil.move(str(git_backup), str(git_dir))

        # 1. Prepare Contexts
        detectors_ctx = self.prepare_detectors_context(rule_set)
        config_ctx = self.prepare_config_context(data_source_config)
        deps_ctx = self.prepare_dependencies_context(rule_set)

        # 2. Define Template Map (Template Name -> Output Filename)
        template_map = {
            "logic/__init__.py.j2": "logic/__init__.py",
            "logic/detectors.py.j2": "logic/detectors.py",
            "logic/processor.py.j2": "logic/processor.py",
            "utils/__init__.py.j2": "utils/__init__.py",
            "utils/data_loader.py.j2": "utils/data_loader.py",
            "main.py.j2": "main.py",
            "config.py.j2": "config.py",
            "pyproject.toml.j2": "pyproject.toml",
            ".gitignore.j2": ".gitignore",
        }

        # 3. Render and Write
        logger.info(f"Rendering {len(template_map)} templates to {output_dir}")
        for tmpl_name, out_name in template_map.items():
            template = self.env.get_template(tmpl_name)

            # Pass all contexts to templates
            rendered_code = template.render(
                project_id=project_id,
                detectors=detectors_ctx,
                config=config_ctx,
                dependencies=deps_ctx["dependencies"],
            )

            # Handle subdirectory creation (e.g., logic/, utils/)
            dest_path = output_dir / out_name
            dest_path.parent.mkdir(parents=True, exist_ok=True)

            with open(dest_path, "w") as f:
                f.write(rendered_code)

        logger.info(f"Child Codebase generated at: {output_dir}")
