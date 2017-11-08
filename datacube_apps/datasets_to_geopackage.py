import click
import fiona
from fiona.crs import from_string
from tqdm import tqdm

import datacube
import datacube.model
from datacube.ui.click import parsed_search_expressions, version_option, verbose_option, pass_index

# To write data with fiona, we first have to define a schema, and the crs, that
# we'll pass as arguments when opening the connection to the file.
# After that, features are added one by one by writting a dictionary representation
# of that feature.
schema = {'geometry': 'Polygon',
          'properties': [('id', 'str'),
                         ('product', 'str'),
                         ('local_path', 'str'),
                         ('obs_date', 'datetime'),
                         ('creation_date', 'datetime')]}

epsg4326 = from_string('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')


# Feature builder
def dataset_to_feature(dataset: datacube.model.Dataset):
    feature = {'geometry': dataset.extent.json,
               'properties': {'id': str(dataset.id),
                              'product': dataset.type.name,
                              'local_path': dataset.local_uri,
                              'obs_date': dataset.center_time,
                              'creation_date': dataset.metadata.creation_dt
                              }}
    return feature


def write_to_disk(filename, datasets, num_datasets):
    # Open connection with gpkg file in a context manager and write features to it
    with fiona.open(filename, 'w',
                    layer='datasets',
                    driver='GPKG',
                    schema=schema,
                    crs=epsg4326) as dst:
        for dataset in tqdm(datasets, 'Exported datasets', total=num_datasets):
            feature = dataset_to_feature(dataset)
            dst.write(feature)


@click.command()
@verbose_option
@version_option
@click.argument('filename', type=click.Path(exists=False, writable=True))
@parsed_search_expressions
@pass_index()
def main(index, filename, expressions):
    """
    Write some datasets and their metadata into a GeoPackage.
    """
    click.echo(f'Search expressions: {expressions}')
    click.echo(f'Output file: {filename}')
    dc = datacube.Datacube(index=index)

    num_datasets = dc.index.datasets.count(**expressions)
    datasets = dc.index.datasets.search(**expressions)

    write_to_disk(filename, datasets, num_datasets)


if __name__ == '__main__':
    main()
