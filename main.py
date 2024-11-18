import sympy as sp

# Define the variables
p = sp.Symbol('p')
N = sp.Symbol('N')

# Define the simplified function
d = (1/2)**(1/p)

# Calculate the derivative with respect to p
ddp = sp.diff(d, p)

# Calculate the derivative with respect to N
ddN = sp.diff(d, N)

print("Derivative with respect to p:", ddp)
print("Derivative with respect to N:", ddN)
