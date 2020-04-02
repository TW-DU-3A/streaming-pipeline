
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class WebHdfsSensor(BaseSensorOperator):
    """
    Waits for a file or folder to land in HDFS
    """
    template_fields = ('filepath',)
    @apply_defaults
    def __init__(self,
                 filepath,
                 webhdfs_conn_id='webhdfs_default',
                 *args,
                 **kwargs):
        super(WebHdfsSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.webhdfs_conn_id = webhdfs_conn_id

    def poke(self, context):
        from airflow.hooks.webhdfs_hook import WebHDFSHook
        c = WebHDFSHook(self.webhdfs_conn_id)
        self.log.info('Poking for file %s', self.filepath)
        return c.check_for_path(hdfs_path=self.filepath)
