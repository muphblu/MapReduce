
def start_map(jobber, text):
    words = text.split('\W+')
    for w in words:
        jobber.write_pair((w, 1))
