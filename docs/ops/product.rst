.. _product-doc:

Product Definition
******************

Product description document defines some of the metadata common to all the datasets belonging to the products.
It also describes the measurements that product has and some of the properties of the measurements.


.. literalinclude:: ../config_samples/dataset_types/dsm1sv10.yaml
   :language: yaml

name
    A short name for a Product. This name is used all the time when specifying what type of
    data to load.

    .. code-block:: python
       :emphasize-lines: 2

       dc = Datacube()
       data = dc.load(product='dsm1sv10')

description
    A long description of a product, describing in text where it has come from, and how it can
    be used.

metadata_type
    Name of the :ref:`metadata-type-definition`. This determines the structure of the Dataset
    documents that will be a part of this Product, and which fields will be available for
    searching.

metadata
    A dictionary containing metadata common to all the datasets in the product.

    It is used when adding datasets to an Index to match datasets to their respective products.

storage (optional)
    Describes some of common storage attributes of all the datasets. While optional defining this will make
    product data easier to access and use.

    crs
        Coordinate reference system common to all the datasets in the product. 'EPSG:<code>' or WKT string.

    resolution
        Resolution of the data of all the datasets in the product specified in projection units.
        Use ``latitude``, ``longitude`` if the projection is geographic and ``x``, ``y`` otherwise

measurements
    A list of measurements in this product. A measurement is a particular variable. Generally a Product
    will contain at least one measurement. Measurements could include different spectral bands of a
    satellite product.

    name
         Name of the measurement

    units
         Units of the measurement

    dtype
         Data type. One of ``(u)int(8,16,32,64), float32, float64``

    nodata
         No data value

    spectral_definition (optional)
         Spectral response of the reflectance measurement.

         .. code-block:: yaml

             spectral_definition:
                  wavelength: [410, 411, 412]
                  response: [0.0261, 0.029, 0.0318]

    flags_definition (optional)
        Bit flag meanings of the bitset 'measurement'

        .. code-block:: yaml

            flags_definition:
                platform:
                  bits: [0,1,2,3]
                  description: 'Platform name'
                  values:
                    0: 'terra'
                    1: 'aqua_terra'
                    2: 'aqua'
                contiguous:
                  bits: 8
                  description: 'All bands for this pixel contain non-null values'
                  values: {0: false, 1: true}
