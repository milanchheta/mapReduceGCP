def WordCountReducer(data):
    resultDict = {}
    for word in data:
        resultDict[word] = len(data[word])
    return resultDict