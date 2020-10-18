"""Tools and constants for categorizing raw IPs
"""

class IpProcessor(object):
    """An object class holding compiled matcher state
    """
    def __init__(self):
        pass

    def process_ip(self, ip):
        """Process an input ip, returning an org and category tuple
        """

        if ("157.240.25." in ip) or ("157.240.24." in ip) or ("157.240.2." in ip):
            return "Facebook", "Unknown (Not Mapped)"

        if ("164.90.115." in ip) or ("164.90.78." in ip) or ("169.136.70." in ip) or ("169.136.65." in ip):
            return "Likee", "Video"

        if ("202.80.222." in ip):
            return "Kabel Mediacom", "Unknown (Not Mapped)"

        if "169.136.65.204" in ip:
            return "QQ", "Messaging"

        if ("107.150.124." in ip) or ("23.248.162." in ip):
            return "UCloud", "Games"

        if (ip.startswith("74.125.")) or (ip.startswith("173.194.")):
            return "Google", "Google (No DNS)"

        if "104.248.148.225" in ip:
            return "Digital Ocean", "IAAS"

        if "8.37.232.4" in ip:
            return "Level3", "IAAS"

        if ip.startswith("168.235.199."):
            return "Quantil Networks", "Mixed CDN"

        return "Unknown (No DNS)", "Unknown (No DNS)"
