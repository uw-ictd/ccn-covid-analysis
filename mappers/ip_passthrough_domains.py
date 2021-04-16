"""Tools and constants for categorizing raw IPs
"""

class PassthroughDomainProcessor(object):
    """An object class holding compiled matcher state
    """
    def __init__(self):
        pass

    def process_ip_fqdn(self, ip, fqdn):
        """Process an input fqdn and ip, returning an org and category tuple
        """

        ip_parts = ip.split(".")

        for part in ip_parts:
            if part not in fqdn:
                return "Unknown (Not Mapped)", "Unknown (Not Mapped)"

        return "Peer (Unknown Org)", "Peer to Peer"
