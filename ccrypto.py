# Databricks notebook source
from cryptography.fernet import Fernet
import os
import hashlib

keyFileName = "key.data"

def getKey():
    if(os.path.exists(keyFileName)) is False :
        raise Exception("key does not exist")
    with open(keyFileName, 'rb') as openfile:
        bts = openfile.read()
        return bts

def encryptDept(deptValue):
    dept = int(deptValue)
    deptBytes = str(dept).encode()
    key = getKey()
    f = Fernet(key)
    token = f.encrypt(deptBytes)
    return token

def decryptDept(deptValue):
    token=deptValue
    key = getKey()
    f = Fernet(key)
    deptBytes = f.decrypt(token)
    deptInt=int(deptBytes)
    return deptInt

def encryptSal(salValue):
    sal = float(salValue)
    salBytes = str(sal).encode()
    key = getKey()
    f = Fernet(key)
    token = f.encrypt(salBytes)

    return token

def decryptSal(salValue):
    token=salValue
    key = getKey()
    f = Fernet(key)
    salBytes = f.decrypt(token)
    salFloat=float(salBytes)
    return salFloat

def hashDept(deptValue):
    dept = int(deptValue)
    deptBytes = str(dept).encode()
    hasho = hashlib.sha224(deptBytes).hexdigest()
    return hasho

def tests():
    test_data1 = [30, 40, 20, 13, 9]

    for x in test_data1:

        e = encryptDept(x)
        d = decryptDept(e)
        print(f"in {x} encrypted {e} out {d}")
        assert d == x

    print("dept tests passed 5/5)")
    print("\n")

    test_data2 = [1000.00, 3434.34 , 3433 , 43432]

    for x in test_data2:
        e = encryptSal(x)
        d = decryptSal(e)
        print(f"in {x} enrypted {e} out {d}")
        assert d == x

    print("sal tests passed 4/4)")
    print("\n")

    test_data3 = [[3,'4cfc3a1811fe40afa401b25ef7fa0379f1f7c1930a04f8755d678474'],
            [12,'3c794f0c67bd561ce841fc6a5999bf0df298a0f0ae3487efda9d0ef4'],
            [26,'958d42a83cf840cde79922f0795fd6ac7da4d2df828edc32244bb3ba'],
            [53,'6c905a484091b8fd5e27b0cbdb51751f6de1f15f2f0d9d1b06149e92']]

    for x in test_data3:
        h = hashDept(x[0]);
        print("input ", x[0] , " expected " , x[1] , " output ", h);

        assert h == x[1]

    print("hash tests passed 4/4")



tests()