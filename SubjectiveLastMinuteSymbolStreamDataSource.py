import time
from subjective_abstract_data_source_package.SubjectiveDataSource import SubjectiveDataSource
from brainboost_data_source_logger_package.BBLogger import BBLogger


class SubjectiveLastMinuteSymbolStreamDataSource(SubjectiveDataSource):
    connection_type = "Stream"
    connection_fields = ["symbol", "interval", "minutes_ago"]
    icon_svg = "<svg width='24' height='24' viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><circle cx='12' cy='12' r='9' fill='#2d6a4f'/><path d='M7 12h10' stroke='#ffffff' stroke-width='2'/></svg>"

    def get_icon(self):
        return self.icon_svg

    def get_connection_data(self):
        return {"connection_type": self.connection_type, "fields": list(self.connection_fields)}

    def notify(self, data):
        self.update(data)

    def _get_param(self, key, default=None):
        return self.params.get(key, default)

    def start(self):
        from com_goldenthinker_trade_datasource.LastMinuteSymbolStreamDataSource import LastMinuteSymbolStreamDataSource

        minutes_ago = int(self._get_param("minutes_ago", 30))
        name = self._get_param("name", "last_min_sequences_all_symbols")
        ds = LastMinuteSymbolStreamDataSource(name=name, minutes_ago=minutes_ago, listener=self)
        ds.start()

    def fetch(self):
        if self.status_callback:
            self.status_callback(self.get_name(), "stream_started")
        self.start()
        self.set_fetch_completed(True)
        BBLogger.log(f"Stream started for {self.get_name()}")
