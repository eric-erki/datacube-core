
.. _geometry-api:

Geometry API
============

Open Data Cube contains a utility API for geometry operations which is also
CRS aware. It uses a similar API and approach as shapely_, by wrapping OGR/OSR
provided by GDAL, and using a GeoJSON_ compatible internal structure, with
additional support for reprojecting geometries between Spatial Reference Systems.

.. currentmodule:: datacube.utils.geometry


Classes
-------

.. autoclass:: Geometry
   :members:

.. autoclass:: GeoBox
   :members:

.. autoclass:: CRS
   :members:

.. autoclass:: BoundingBox
   :members:

Constructors
------------

This set of functions provide a simple way to create :class:`Geometry` objects. Instead
of having to manually specify GeoJSON data structures.

.. autofunction:: point
.. autofunction:: multipoint
.. autofunction:: line
.. autofunction:: multiline
.. autofunction:: polygon
.. autofunction:: multipolygon
.. autofunction:: box
.. autofunction:: polygon_from_transform


Multi-geometry operations
-------------------------

.. autofunction:: unary_union
.. autofunction:: unary_intersection


.. _shapely: https://github.com/Toblerity/Shapely
.. _GeoJSON: http://geojson.org
