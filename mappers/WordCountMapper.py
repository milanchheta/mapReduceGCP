def WordCountMapper(data, filename):
    data = data.lower()
    dataList = ''.join((c if c.isalpha() else ' ') for c in data).split()
    resultData = []
    for word in dataList:
        resultData.append((word, 1))
    return resultData