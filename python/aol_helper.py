from arcgis import GIS
import os
import zipfile
import tempfile
import glob


class ArcGISOnlineHelper(object):

    def __init__(self, url, username, password, analysis_folder=None, common_tags=None):
        self.gis = GIS(url=url, username=username, password=password)
        if analysis_folder:
            create_folder_result = self.gis.content.create_folder(analysis_folder)
            self.analysis_folder = analysis_folder
        else:
            self.analysis_folder = None
        if common_tags and isinstance(common_tags, list):
            self.common_tags = common_tags
        else:
            self.common_tags = []

    def get_item(self, layer_properties):
        """
        get arcgis online resource by title and item type
        """
        if 'title' not in layer_properties:
            raise Exception('layer_properties must define title')

        if 'type' in layer_properties:
            search_results = self.gis.content.search(query="title:%s" % layer_properties['title'],
                                                    item_type=layer_properties['type'])
        else:
            search_results = self.gis.content.search(query="title:%s" % layer_properties['title'])

        for search_result in search_results:
            if search_result.title == layer_properties['title']:
                return search_result
        return None

    def get_or_save_item(self, layer_properties, data, delete_existing=False):
        """
        get existing item and save it if does not exist
        """
        existing_item = self.get_item(layer_properties)
        if existing_item:
            if delete_existing:
                existing_item.delete()
            else:
                return existing_item, False

        item = self.gis.content.add(item_properties=layer_properties, data=data, folder=self.analysis_folder)
        return item, True

    def delete_if_exists_item(self, layer_properties):
        """
        delete arcgis online resource
        """
        item = self.get_item(layer_properties)
        if item:
            item.delete()
            return True
        return False

    def publish_item(self, item, publish_options=None, over_write=False):
        """
        publish shapefile (zipped) or csv file (with addresses) as a feature layer
        """
        search_results = self.gis.content.search(query="title:%s" % item['title'], item_type='Feature Layer')
        for search_result in search_results:
            if search_result.title == item['title']:
                if over_write:
                    search_result.delete()
                else:
                    return search_result, False

        layer_item = item.publish(publish_options)
        return layer_item, True

    def move_to_analysis_folder(self, item):
        result = item.move(self.analysis_folder)
        return result["success"]

    def upload_csv(self, csv_file, layer_name, tags=None):
        if isinstance(tags, list):
            tags = self.common_tags + tags
        else:
            tags = self.common_tags

        _item = self.gis.content.add({'title': layer_name, 'tags': tags}, data=csv_file)
        _table = _item.publish(publish_parameters=None, address_fields=None)
        return _table

    def upload_shapefile(self, shapefile_path, layer_name=None, tags=None):
        tmpdir = tempfile.gettempdir()
        shp_files = glob.glob("%s.*" % os.path.splitext(shapefile_path)[0])
        zip_fname = os.path.join(tmpdir,
                                 "%s.zip" % os.path.splitext(os.path.basename(shapefile_path))[0])
        zipper = zipfile.ZipFile(zip_fname, 'w', zipfile.ZIP_DEFLATED)
        for file in shp_files:
            zipper.write(file)
        zipper.close() # never leave it open

        if isinstance(tags, list):
            tags = self.common_tags + tags
        else:
            tags = self.common_tags

        item = self.gis.content.add({'type': 'Shapefile', 'title': layer_name, 'tags': tags}, data=zip_fname)
        flayer = item.publish()
        return flayer

