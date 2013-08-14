import re
import urllib
import urlparse
import sys
import logging
import uuid
import mimetypes
from datetime import datetime

from string import Template

from ckan import model

from ckan import plugins as p
from ckan import model
from ckan.lib.helpers import json
from ckan import logic
from ckan.lib.navl.validators import not_empty


from ckan.plugins.core import SingletonPlugin, implements

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra

from owslib.wfs import WebFeatureService
from owslib.wms import WebMapService
from lxml import etree
from ckanext.spatial.harvesters.base import SpatialHarvester, text_traceback
import urllib
import json
from urllib import urlencode

class WMSWFSHarvester(SpatialHarvester, SingletonPlugin):
    '''
    A Harvester for WMS/WFS servers
    '''
    implements(IHarvester)

    wms=None
    wfs=None

    def info(self):
        return {
            'name': 'WMS/WFS',
            'title': 'WMS/WFS',
            'description': 'A Server instance that implements OGC\'s OWS standards (WFS/WMS)'
            }


    def get_original_url(self, harvest_object_id):
        obj = model.Session.query(HarvestObject).\
                                    filter(HarvestObject.id==harvest_object_id).\
                                    first()
        return obj.source.url

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.WMSWFS.gather')
        log.debug('WMSWFSHarvester gather_stage for job: %r', harvest_job)
        # Get source URL
        url = harvest_job.source.url

        self._set_source_config(harvest_job.source.config)

        try:
            self._setup_wmswfs_client(url)
        except Exception, e:
            self._save_gather_error('Error contacting the WMSWFS server: %s' % e, harvest_job)
            return None

        query = model.Session.query(HarvestObject.guid, HarvestObject.package_id).\
                                    filter(HarvestObject.current==True).\
                                    filter(HarvestObject.harvest_source_id==harvest_job.source.id)
        guid_to_package_id = {}

        for guid, package_id in query:
            guid_to_package_id[guid] = package_id

        guids_in_db = set(guid_to_package_id.keys())

        log.debug('Starting gathering for %s' % url)
        guids_in_harvest = set()
	contents = []
	if self.wfs.contents:
		contents = contents + self.wfs.contents.keys()
#		contents = contents + [x.split(":",1)[1] for x in self.wfs.contents.keys()] # strip namespace: prefix
#	if self.wms.contents:
#		contents = contents + self.wms.contents.keys()
        try:
            for identifier in contents:
                try:
                    log.info('Got identifier %s from the WMS/WFS Server', identifier)
                    if identifier is None:
                        log.error('WMS/WFS returned identifier %r, skipping...' % identifier)
                        continue
                    guids_in_harvest.add(identifier)
                except Exception, e:
                    self._save_gather_error('Error for the identifier %s [%r]' % (identifier,e), harvest_job)
                    continue


        except Exception, e:
            log.error('Exception: %s' % text_traceback())
            self._save_gather_error('Error gathering the identifiers from the WMSWFS server [%s]' % str(e), harvest_job)
            return None

        new = guids_in_harvest - guids_in_db
        delete = guids_in_db - guids_in_harvest
        change = guids_in_db & guids_in_harvest

        ids = []
        for guid in new:
            obj = HarvestObject(guid=guid, job=harvest_job,
                                extras=[HOExtra(key='status', value='new')])
            obj.save()
            ids.append(obj.id)
        for guid in change:
            obj = HarvestObject(guid=guid, job=harvest_job,
                                package_id=guid_to_package_id[guid],
                                extras=[HOExtra(key='status', value='change')])
            obj.save()
            ids.append(obj.id)
        for guid in delete:
            obj = HarvestObject(guid=guid, job=harvest_job,
                                package_id=guid_to_package_id[guid],
                                extras=[HOExtra(key='status', value='delete')])
            ids.append(obj.id)
            model.Session.query(HarvestObject).\
                  filter_by(guid=guid).\
                  update({'current': False}, False)
            obj.save()

        if len(ids) == 0:
            self._save_gather_error('No records received from the WMSWFS server', harvest_job)
            return None

        return ids

    def fetch_stage(self,harvest_object):
        log = logging.getLogger(__name__ + '.WMSWFS.fetch')
        log.debug('WMSWFSHarvester fetch_stage for object: %s', harvest_object.id)

        url = harvest_object.source.url
        try:
            self._setup_wmswfs_client(url)
        except Exception, e:
            self._save_object_error('Error contacting the WMSWFS server: %s' % e,
                                    harvest_object)
            return False

        identifier = harvest_object.guid
        try:
	    #save metadata record as json 
	    record = None
	    metadata = None
	    if identifier in self.wms.contents.keys():
		metadata = self.wms[identifier]
	    if identifier in self.wfs.contents.keys():
		metadata = self.wfs[identifier]
	    if metadata != None:
		    if metadata.boundingBoxWGS84 != None:
			bbox = {"minx":metadata.boundingBoxWGS84[0],"miny":metadata.boundingBoxWGS84[1],"maxx":metadata.boundingBoxWGS84[2],"maxy":metadata.boundingBoxWGS84[3]}
		    else: 
			bbox = {"minx":metadata.boundingBox[0],"miny":metadata.boundingBox[1],"maxx":metadata.boundingBox[2],"maxy":metadata.boundingBox[3]}
	            contents = {
			"metadata": {
			"id": metadata.id, "title": metadata.title, "abstract": metadata.abstract, "keywords": metadata.keywords, "bbox": bbox}}
 		    if identifier in self.wms.contents.keys() or identifier.split(":",1)[1] in self.wms.contents.keys():
		        contents["wmsurl"]= self.wms.getOperationByName('GetMap').methods['Get']['url']
		        contents["wmsformats"]= self.wms.getOperationByName('GetMap').formatOptions
		    if identifier in self.wfs.contents.keys():
		        contents["wfsurl"]= self.wfs.getOperationByName('GetFeature').methods['Get']['url']
		        #"wfsformats": self.wfs.getOperationByName('GetFeature').formatOptions,
			#HACKHACKHACK formatOptions doesn't seem to get parsed correctly by owslib, hardcodings
			contents["wfsformats"]= ["SHAPE-ZIP","csv","json"]
	            record = json.dumps(contents)

        except Exception, e:
	    log.debug(sys.exc_info()[0])
            self._save_object_error('Error getting the WMSWFS record with GUID %s' % identifier, harvest_object)
            return False

        if record is None:
            self._save_object_error('Empty record for GUID %s' % identifier,
                                    harvest_object)
            return False

        try:
            harvest_object.content = record
            harvest_object.save()
        except Exception,e:
            self._save_object_error('Error saving the harvest object for GUID %s [%r]' % \
                                    (identifier, e), harvest_object)
            return False

        log.debug('WMS/WFS GET URL saved (len %s)', len(record))
        return True

    def _setup_wmswfs_client(self, url):
        self.wfs = WebFeatureService(url, version='1.1.0')
	self.wms = WebMapService(url, version='1.1.1')

    def import_stage(self, harvest_object):

        log = logging.getLogger(__name__ + '.import')
        log.debug('Import stage for harvest object: %s', harvest_object.id)

        if not harvest_object:
            log.error('No harvest object received')
            return False

        self._set_source_config(harvest_object.source.config)

        if self.force_import:
            status = 'change'
        else:
            status = self._get_object_extra(harvest_object, 'status')

        # Get the last harvested object (if any)
        previous_object = model.Session.query(HarvestObject) \
                          .filter(HarvestObject.guid==harvest_object.guid) \
                          .filter(HarvestObject.current==True) \
                          .first()

        if status == 'delete':
            # Delete package
            context = {'model': model, 'session': model.Session, 'user': self._get_user_name()}

            p.toolkit.get_action('package_delete')(context, {'id': harvest_object.package_id})
            log.info('Deleted package {0} with guid {1}'.format(harvest_object.package_id, harvest_object.guid))

            return True

        # Flag previous object as not current anymore
        if previous_object and not self.force_import:
            previous_object.current = False
            previous_object.add()

        # Generate GUID if not present (i.e. it's a manual import)
        if not harvest_object.guid:
            m = hashlib.md5()
            m.update(harvest_object.content.encode('utf8', 'ignore'))
            harvest_object.guid = m.hexdigest()
            harvest_object.add()

        # Build the package dict
        package_dict = self.get_package_dict(None, harvest_object)
        if not package_dict:
            log.error('No package dict returned, aborting import for object {0}'.format(harvest_object.id))
            return False

        # Create / update the package

        context = {'model': model,
                   'session': model.Session,
                   'user': self._get_user_name(),
                   'extras_as_string': True,
                   'api_version': '2',
                   'return_id_only': True}

        # The default package schema does not like Upper case tags
        tag_schema = logic.schema.default_tags_schema()
        tag_schema['name'] = [not_empty, unicode]

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        if status == 'new':
            package_schema = logic.schema.default_create_package_schema()
            package_schema['tags'] = tag_schema
            context['schema'] = package_schema

            # We need to explicitly provide a package ID, otherwise ckanext-spatial
            # won't be be able to link the extent to the package.
            package_dict['id'] = unicode(uuid.uuid4())
            package_schema['id'] = [unicode]

            # Save reference to the package on the object
            harvest_object.package_id = package_dict['id']
            harvest_object.add()
            # Defer constraints and flush so the dataset can be indexed with
            # the harvest object id (on the after_show hook from the harvester
            # plugin)
            model.Session.execute('SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
            model.Session.flush()

            try:
                package_id = p.toolkit.get_action('package_create')(context, package_dict)
                log.info('Created new package %s with guid %s', package_id, harvest_object.guid)
            except p.toolkit.ValidationError, e:
                self._save_object_error('Validation Error: %s' % str(e.error_summary), harvest_object, 'Import')
                return False

        elif status == 'change':

            # Check if the modified date is more recent
            if not self.force_import and harvest_object.metadata_modified_date <= previous_object.metadata_modified_date:

                # Assign the previous job id to the new object to
                # avoid losing history
                harvest_object.harvest_job_id = previous_object.job.id
                harvest_object.add()

                # Delete the previous object to avoid cluttering the object table
                previous_object.delete()

                log.info('Document with GUID %s unchanged, skipping...' % (harvest_object.guid))
            else:
                package_schema = logic.schema.default_update_package_schema()
                package_schema['tags'] = tag_schema
                context['schema'] = package_schema

                package_dict['id'] = harvest_object.package_id
                try:
                    package_id = p.toolkit.get_action('package_update')(context, package_dict)
                    log.info('Updated package %s with guid %s', package_id, harvest_object.guid)
                except p.toolkit.ValidationError, e:
                    self._save_object_error('Validation Error: %s' % str(e.error_summary), harvest_object, 'Import')
                    return False

        model.Session.commit()

        return True

    def get_package_dict(self, iso_values, harvest_object):
        '''
        Constructs a package_dict suitable to be passed to package_create or
        package_update. See documentation on
        ckan.logic.action.create.package_create for more details

        Tipically, custom harvesters would only want to add or modify the
        extras, but the whole method can be replaced if necessary. Note that
        if only minor modifications need to be made you can call the parent
        method from your custom harvester and modify the output, eg:

            class MyHarvester(SpatialHarvester):

                def get_package_dict(self, iso_values, harvest_object):

                    package_dict = super(MyHarvester, self).get_package_dict(iso_values, harvest_object)

                    package_dict['extras']['my-custom-extra-1'] = 'value1'
                    package_dict['extras']['my-custom-extra-2'] = 'value2'

                    return package_dict

        If a dict is not returned by this function, the import stage will be cancelled.

        :param iso_values: Dictionary with parsed values from the ISO 19139
            XML document
        :type iso_values: dict
        :param harvest_object: HarvestObject domain object (with access to
            job and source objects)
        :type harvest_object: HarvestObject

        :returns: A dataset dictionary (package_dict)
        :rtype: dict
        '''
	if harvest_object.content == None or harvest_object.content == '':
		raise Exception('No harvest content')
	content =  json.loads(harvest_object.content)
	metadata = content['metadata']
        tags = []
        if 'keywords' in metadata:
            for tag in metadata['keywords']:
                tag = tag[:50] if len(tag) > 50 else tag
                tags.append({'name': tag})

        package_dict = {
            'title': metadata['title'],
            'notes': metadata['abstract'],
            'tags': tags,
            'resources': [],
        }

        # We need to get the owner organization (if any) from the harvest
        # source dataset
        source_dataset = model.Package.get(harvest_object.source.id)
        if source_dataset.owner_org:
            package_dict['owner_org'] = source_dataset.owner_org

        # Package name
        package = harvest_object.package
        if package is None or package.title != metadata['title']:
            name = self._gen_new_name(metadata['title'])
            if not name:
                raise Exception('Could not generate a unique name from the title or the GUID. Please choose a more unique title.')
            package_dict['name'] = name
        else:
            package_dict['name'] = package.name

        extras = {
            'spatial_harvester': True,
	#	    'spatial-reference-system': metadata['defaultSRS']
        }

	#        extras['licence'] = 
	#        extras['access_constraints'] 

        # Grpahic preview
        browse_graphic = metadata.get('browse-graphic')
        if browse_graphic:
            browse_graphic = browse_graphic[0]
            extras['graphic-preview-file'] = browse_graphic.get('file')
            if browse_graphic.get('description'):
                extras['graphic-preview-description'] = browse_graphic.get('description')
            if browse_graphic.get('type'):
                extras['graphic-preview-type'] = browse_graphic.get('type')


	#save bbox
	bbox = metadata.get('bbox')
        if bbox:
            extras['bbox-east-long'] = bbox['minx']
            extras['bbox-north-lat'] = bbox['maxy']
            extras['bbox-south-lat'] = bbox['miny']
            extras['bbox-west-long'] = bbox['maxx']

            try:
                xmin = float(bbox['minx'])
                xmax = float(bbox['maxx'])
                ymin = float(bbox['miny'])
                ymax = float(bbox['maxy'])
            except ValueError, e:
                self._save_object_error('Error parsing bounding box value: {0}'.format(str(e)),
                                    harvest_object, 'Import')
            else:
                # Construct a GeoJSON extent so ckanext-spatial can register the extent geometry

                # Some publishers define the same two corners for the bbox (ie a point),
                # that causes problems in the search if stored as polygon
                if xmin == xmax or ymin == ymax:
                    extent_string = Template('{"type": "Point", "coordinates": [$x, $y]}').substitute(
                        x=xmin, y=ymin
                    )
                    self._save_object_error('Point extent defined instead of polygon',
                                     harvest_object, 'Import')
                else:
                    extent_string = self.extent_template.substitute(
                        xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax
                    )

                extras['spatial'] = extent_string.strip()
        else:
            log.debug('No spatial extent defined for this object')

	#define resources/files/URLs
	# WFS link, WMS link, KML, SHP, CSV and GeoJSON 
	resource_locators = []

	if 'wmsformats' in content.keys():
	        resource_locators.append({"name":metadata['title'] + " - Preview this Dataset (WMS)","description":"View the data in this datasets online via we-based WMS viewer","format":"wms",
		"url":content['wmsurl']+"version=1.1.0&request=GetCapabilities"})
		for format in content['wmsformats']:
			url = content['wmsurl']+"version=1.1.0&request=GetMap&layers="+metadata['id']+"&bbox="+repr(bbox['minx'])+","+repr(bbox['miny'])+","+repr(bbox['maxx'])+","+repr(bbox['maxy'])+"&width=512&height=512&format="+urllib.quote(format)
			if format == "kml":
		        	resource_locators.append({"name":metadata['title'] + " KML","description":"For use in web and desktop spatial data tools including Google Earth" ,"format":format,"url":url})
	if 'wfsformats' in content.keys():
  		for format in content['wfsformats']:
			url = content['wfsurl']+"?service=WFS&version=1.0.0&request=GetFeature&typeName="+metadata['id']+"&outputFormat="+urllib.quote(format)
			if format == "shp" or format == "SHAPE-ZIP":
			        resource_locators.append({"name": metadata['title'] + " Shapefile","description":"For use in Desktop GIS tools","format":format,"url":url})
			if format == "csv":
			        resource_locators.append({"name": metadata['title'] + " CSV","description":"For summary of the objects/data in this collection","format":format,"url":url})
			if format == "json":
			        resource_locators.append({"name":metadata['title'] + " GeoJSON","description":"For use in web-based data visualisation of this collection","format":format,"url":url})
	        resource_locators.append({"name":metadata['title'] + " WFS Link","description":"WFS Link for use of live data in Desktop GIS tools","format":"wfs",
		"url":content['wfsurl']+"?service=WFS&version=1.0.0&request=GetCapabilities"})



        if len(resource_locators):
            for resource_locator in resource_locators:
                url = resource_locator.get('url', '').strip()
                if url:
                    resource = {}
                    resource['format'] = guess_resource_format(url)
                    if resource['format'] == 'wms':
                        # Check if the service is a view service
                        test_url = url.split('?')[0] if '?' in url else url
                        if self._is_wms(test_url):
                            resource['verified'] = True
                            resource['verified_date'] = datetime.now().isoformat()

                    resource.update(
                        {
                            'url': url,
                            'name': resource_locator.get('name') or p.toolkit._('Unnamed resource'),
                            'description': resource_locator.get('description') or  '',
                        })
                    package_dict['resources'].append(resource)


        extras_as_dict = []
        for key, value in extras.iteritems():
            if isinstance(value, (list, dict)):
                extras_as_dict.append({'key': key, 'value': json.dumps(value)})
            else:
                extras_as_dict.append({'key': key, 'value': value})

        package_dict['extras'] = extras_as_dict

        return package_dict
def guess_resource_format(url, use_mimetypes=True):
    '''
    Given a URL try to guess the best format to assign to the resource

    The function looks for common patterns in popular geospatial services and
    file extensions, so it may not be 100% accurate. It just looks at the
    provided URL, it does not attempt to perform any remote check.

    if 'use_mimetypes' is True (default value), the mimetypes module will be
    used if no match was found before.

    Returns None if no format could be guessed.

    '''
    url = url.lower().strip()

    file_types = {
        'kmz': ('kmz',),
        'gml': ('gml',),
        'kml' : ('kml',),
        'csv' : ('csv',),
        'geojson' : ('json',),
        'shp' : ('shape-zip',"shp"),
    }

    for file_type, extensions in file_types.iteritems():
        for extension in extensions:
		if extension in url:
	            return file_type

    resource_format, encoding = mimetypes.guess_type(url)
    if resource_format:
        return resource_format

    resource_types = {
        # OGC
        'wms': ('service=wms', 'geoserver/wms', 'mapserver/wmsserver', 'com.esri.wms.Esrimap'),
        'wfs': ('service=wfs', 'geoserver/wfs', 'mapserver/wfsserver', 'com.esri.wfs.Esrimap'),
        'wcs': ('service=wcs', 'geoserver/wcs', 'imageserver/wcsserver', 'mapserver/wcsserver'),
        'sos': ('service=sos',),
        'csw': ('service=csw',),
        # ESRI
        'kml': ('mapserver/generatekml',),
        'arcims': ('com.esri.esrimap.esrimap',),
        'arcgis_rest': ('arcgis/rest/services',),
    }

    for resource_type, parts in resource_types.iteritems():
        if any(part in url for part in parts):
            return resource_type


    return None
