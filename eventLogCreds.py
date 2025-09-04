import base64,hashlib
def md5string(string):
    m = hashlib.md5()
    m.update(string)
    return m.hexdigest()
encodedauth = base64.b64encode(("first_user@capillarytech.com" + ':' + md5string("j#E=ges?fRNp20".encode('utf-8'))).encode('utf-8'))
print(encodedauth)
print("jwndjkn")
print(encodedauth.decode('utf-8'))
endPoint="https://apac.api.capillarytech.com/v3/webHooks/eventLog/requestId"
pathParams=[926_581626819]
endPoint += '/'+'/'.join([str(ele) for ele in pathParams])
print(endPoint)
