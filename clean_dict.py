import pickle
import lzma

def get_dnslog(filename):

    dnslog = dict()
    i = 0

    # getting the original dictionary with addresses as keys and domain names as values
    with lzma.open(filename) as f:
        while True:
            try:
                temp = pickle.load(f)
            except EOFError:
                break
            for addr in temp['response_addresses']:
                if addr not in dnslog.keys():
                    dnslog[addr] = temp['host']
            i += 1
    
    new_dnslog = dict()

    # from the original dictionary, sorting into some of the biggest sites and creating
    # a list of all related domain names
    for entry in dnslog:
        if 'google' in dnslog[entry] or 'gmail' in dnslog[entry]:
            if 'google' not in new_dnslog:
                new_dnslog['google'] = list()
            if dnslog[entry] not in new_dnslog['google']:
                new_dnslog['google'].append(dnslog[entry])
        elif 'fbcdn' in dnslog[entry] or 'facebook' in dnslog[entry] or 'fbsbx' in dnslog[entry]:
            if 'facebook' not in new_dnslog:
                new_dnslog['facebook'] = list()
            if dnslog[entry] not in new_dnslog['facebook']:
                new_dnslog['facebook'].append(dnslog[entry])
        elif 'whatsapp' in dnslog[entry]:
            if 'whatsapp' not in new_dnslog:
                new_dnslog['whatsapp'] = list()
            if dnslog[entry] not in new_dnslog['whatsapp']:
                new_dnslog['whatsapp'].append(dnslog[entry])
        elif 'twimg' in dnslog[entry] or 'twitter' in dnslog[entry]:
            if 'twitter' not in new_dnslog:
                new_dnslog['twitter'] = list()
            if dnslog[entry] not in new_dnslog['twitter']:
                new_dnslog['twitter'].append(dnslog[entry])
        elif 'instagram' in dnslog[entry]:
            if 'instagram' not in new_dnslog:
                new_dnslog['instagram'] = list()
            if dnslog[entry] not in new_dnslog['instagram']:
                new_dnslog['instagram'].append(dnslog[entry])
        elif 'ytimg' in dnslog[entry] or 'youtube' in dnslog[entry]:
            if 'youtube' not in new_dnslog:
                new_dnslog['youtube'] = list()
            if dnslog[entry] not in new_dnslog['youtube']:
                new_dnslog['youtube'].append(dnslog[entry])
        elif 'wikipedia' in dnslog[entry]:
            if 'wikipedia' not in new_dnslog:
                new_dnslog['wikipedia'] = list()
            if dnslog[entry] not in new_dnslog['wikipedia']:
                new_dnslog['wikipedia'].append(dnslog[entry])
        elif 'akamai' in dnslog[entry]:
            if 'akamai cdn' not in new_dnslog:
                new_dnslog['akamai cdn'] = list()
            if dnslog[entry] not in new_dnslog['akamai cdn']:
                new_dnslog['akamai cdn'].append(dnslog[entry])
        elif 'amazonaws' in dnslog[entry] or 'aws' in dnslog[entry]:
            if 'amazon cdn' not in new_dnslog:
                new_dnslog['amazon cdn'] = list()
            if dnslog[entry] not in new_dnslog['amazon cdn']:
                new_dnslog['amazon cdn'].append(dnslog[entry])
        elif 'cloudfront' in dnslog[entry]:
            if 'cloudfront cdn' not in new_dnslog:
                new_dnslog['cloudfront cdn'] = list()
            if dnslog[entry] not in new_dnslog['cloudfront cdn']:
                new_dnslog['cloudfront cdn'].append(dnslog[entry])
        elif 'cloudflare' in dnslog[entry]:
            if 'cloudflare cdn' not in new_dnslog:
                new_dnslog['cloudflare cdn'] = list()
            if dnslog[entry] not in new_dnslog['cloudflare cdn']:
                new_dnslog['cloudflare cdn'].append(dnslog[entry])

    with open('data/oaxaca_dnslog.pickle', mode="wb") as g:
        pickle.dump(new_dnslog, g, protocol=pickle.HIGHEST_PROTOCOL)

if __name__ == '__main__':
    get_dnslog('data/2019-05-17-dnslog_archive-000.xz')