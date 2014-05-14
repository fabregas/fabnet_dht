
import os
from datetime import datetime

from fabnet.core.operator import Operator
from fabnet.utils.logger import oper_logger as logger
from fabnet.utils.safe_json_file import SafeJsonFile
from fabnet.operations.notify_operation import NotifyOperation

NOTIFICATIONS_DB = 'notifications.db'

class TestNotifyOperation(NotifyOperation):
    def on_network_notify(self, event_type, event_provider, event_topic, event_message):
        self._lock()
        try:
            db = SafeJsonFile(os.path.join(self.home_dir, NOTIFICATIONS_DB))
            data = db.read()
            n_list = data.get('notifications', [])
            n_list.append({'event_type': event_type, 'event_provider': event_provider, \
                    'event_topic': event_topic, 'event_message': event_message, 'notify_dt': datetime.now().isoformat()})
            data['notifications'] = n_list
            db.write(data)
        finally:
            self._unlock()
        

class TestMonitorOperator(Operator):
    def __init__(self, self_address, home_dir='/tmp/', key_storage=None, \
                        is_init_node=False, node_name='unknown', config={}):

        Operator.__init__(self, self_address, home_dir, key_storage, \
                                        is_init_node, node_name, config)



OPERLIST = [TestNotifyOperation]
TestMonitorOperator.update_operations_list(OPERLIST)
