import numpy as np
import matplotlib.pyplot as plt
x = [2,3,4]
y = [8,7,5]
plt.figure(figsize=(8,4)) #创建绘图对象
plt.plot(x,y,"b--",linewidth=1)   #在当前绘图对象绘图（X轴，Y轴，蓝色虚线，线宽度）

plt.xlabel("Number of Node") #X轴标签

plt.ylabel("Time/Min")  #Y轴标签

plt.title("RunTime") #图标题

plt.show()  #显示图

plt.savefig('runtime.jpg')