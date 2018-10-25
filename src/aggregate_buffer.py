import constants
import mongoDBI


class aggregate_buffer:
    buffer = None
    max_buffer_count = 25
    dbi = None
    feature = None

    def __init__(self, db_name, feature):
        self.dbi = mongoDBI.mongoDBI (db_name)
        self.feature = feature
        self.buffer = []

    def insert_to_buffer(self, dict):
        self.buffer.append (dict)
        return;

    def write_buffer(self, flush = False):

        cur_len = len (self.buffer)
        if cur_len < self.max_buffer_count and flush == False :
            return;

        self.dbi.insert_bulk ( {self.feature : self.buffer} )
        # Empty buffer

        self.buffer = [ ]
        return;

    # ---------------------------------------------------------- #
    #
    # data_map : dictionary
    # 1st entry is the date or year_week id [ key : constants.label_year_week ]
    # 2nd entry is the data [ key : constants.label_value ]
    #

    def insert_to_db(self, data, label=constants.label_year_week):

        if data is None or len (data) == 0:
            return;

        id = data[ label ]
        data.pop (label, None)
        key_label = label
        key_contents = id
        value_label = constants.label_value
        value_contents = data[ value_label ]
        dict = mongoDBI.mongoDBI.get_insert_dict (key_label, key_contents, value_label, value_contents)
        self.insert_to_buffer (dict)

        self.write_buffer ()
        return;

    def flush(self):
        self.write_buffer(True)
        return;

