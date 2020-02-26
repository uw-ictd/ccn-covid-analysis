from collections import namedtuple


TypicalFlow = namedtuple("TypicalFlow", [
    "start",
    "end",
    "user",
    "dest_ip",
    "user_port",
    "dest_port",
    "bytes_up",
    "bytes_down",
    "protocol",
])

AnomalyPeerToPeerFlow = namedtuple("AnomalyPeerToPeerFlow", [
    "start",
    "end",
    "user_a",
    "user_b",
    "a_port",
    "b_port",
    "bytes_a_to_b",
    "bytes_b_to_a",
    "protocol",
])

AnomalyNoUserFlow = namedtuple("AnomalyNoUserFlow", [
    "start",
    "end",
    "ip_a",
    "ip_b",
    "a_port",
    "b_port",
    "bytes_a_to_b",
    "bytes_b_to_a",
    "protocol",
])
