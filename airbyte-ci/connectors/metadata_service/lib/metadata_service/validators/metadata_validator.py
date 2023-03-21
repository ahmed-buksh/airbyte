import click
import yaml
from pydantic import ValidationError
from metadata_service.models.generated.ConnectorMetadataDefinitionV0 import ConnectorMetadataDefinitionV0


@click.command()
@click.argument("file_path")
def validate_metadata_file(file_path):
    """
    Validates a metadata YAML file against a metadata Pydantic model.
    """
    with open(file_path, "r") as f:
        try:
            metadata = yaml.safe_load(f)
            ConnectorMetadataDefinitionV0.parse_obj(metadata)
            click.echo(f"{file_path} is a valid ConnectorMetadataDefinitionV0 YAML file.")
        except ValidationError as e:
            click.echo(f"{file_path} is not a valid ConnectorMetadataDefinitionV0 YAML file.")
            click.echo(str(e))
            exit(1)