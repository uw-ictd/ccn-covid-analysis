import toml


def read_config(location=None):
    """Read a platform config file or substitute defaults
    """

    if location is None:
        location = "platform-conf.toml"

    try:
        with open(location) as f:
            config = toml.load(f)
            print("Parsed config: {}".format(config))
            return Platform(large_compute_support=config["capabilities"]["large_compute"],
                            altair_support=config["capabilities"]["altair_render"],
                            max_memory_GB=config["system"]["max_memory_GB"],
                            max_processors=config["system"]["processor_count"])
    except FileNotFoundError:
        print("Using the default config values")
        return Platform()


class Platform(object):
    """Represents platform capabilities"""
    def __init__(
            self,
            large_compute_support=True,
            altair_support=True,
            max_memory_GB=8.0,
            max_processors=2,
    ):
        self._large_compute_support = large_compute_support
        self._altair_support = altair_support
        self._max_memory_GB = max_memory_GB
        self._max_processors = max_processors

    @property
    def altair_support(self):
        return self._altair_support

    @property
    def large_compute_support(self):
        return self._large_compute_support

    @property
    def max_processors(self):
        return self._max_processors

    @property
    def max_memory_gigabytes(self):
        return self._max_memory_GB
