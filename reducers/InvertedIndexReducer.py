def InvertedIndexReducer(data):

    resultDict = {}
    for word in data:
        for entry in data[word]:
            if word in resultDict:
                if entry[1] in resultDict[word]:
                    resultDict[word][entry[1]] += 1
                else:
                    resultDict[word][entry[1]] = 1
            else:
                resultDict[word] = {entry[1]: 1}
    return resultDict