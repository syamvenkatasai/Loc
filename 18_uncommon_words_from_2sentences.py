s1="hi hello syam"
s2="hi hello sai"
s1=s1.split()
s2=s2.split()
print(s1)
print(s2)
#s1+=s2
#print(s1)
s3=[]
for i in s1:
	for j in s2:
		if(i in s2 or j in s1) :
			continue
		else:
			s3.append(i)
			s3.append(j)
print(s3)
		
