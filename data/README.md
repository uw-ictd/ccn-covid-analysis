# Data layout for the bok analysis project

Data is contained in 2 main directories, the clean directory and the originals
directory. The originals files are the raw files from Bokondini, and have some
issues. These issues are addressed in the `canonicalize_data.py` module which
generates the cleaned and consolidated dataset in the clean directory.

## Steps taken to clean data

1. Remove nil characters from the transactions log. This is likely due to nodejs
buffering the file and then having the power yanked out from underneath it.

2. Ensure the transaction log and flowlogs have overlapping sets of user ids.
This was an issue at first, but flowlogs have been re-exported now with matching
keys.

3. Separate out anomalous flows. Some flows made it onto the tunnel interface
with bogus source addresses. These may have been spoofed as part of the DOS
attack on UMS.

4. Separate out peer-to-peer flows. I'm not sure exactly what to do with these
yet.

5. Canonicalise the direction of the flows and store in a specific flow type.

6. Convert IP addresses objects to strings (to allow native serialization and
comparison)

7. Concatenate the flowlogs retrieved at different times into one consolidated
log.

8. Remove duplicates from the consolidated log.

9. Add FQDN information where available, either from a previous DNS request by
the user, or if not available, via a reverse DNS lookup.
