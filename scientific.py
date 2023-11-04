import tkinter as tk
from tkinter import ttk
import math
from ttkthemes import ThemedStyle

def evaluate_expression():
    try:
        result = str(eval(entry.get()))
        entry.delete(0, tk.END)
        entry.insert(tk.END, result)
    except Exception:
        entry.delete(0, tk.END)
        entry.insert(tk.END, "Error")

def button_click(event):
    current = entry.get()
    text = event.widget.cget("text")
    if text == "=":
        evaluate_expression()
    elif text == "C":
        entry.delete(0, tk.END)
    elif text == "sqrt":
        entry.delete(0, tk.END)
        entry.insert(tk.END, str(math.sqrt(float(current))))
    elif text == "x^2":
        entry.delete(0, tk.END)
        entry.insert(tk.END, str(float(current) ** 2))
    elif text == "sin":
        entry.delete(0, tk.END)
        entry.insert(tk.END, str(math.sin(math.radians(float(current)))))
    elif text == "cos":
        entry.delete(0, tk.END)
        entry.insert(tk.END, str(math.cos(math.radians(float(current)))))
    elif text == "tan":
        entry.delete(0, tk.END)
        entry.insert(tk.END, str(math.tan(math.radians(float(current)))))
    elif text == "log":
        entry.delete(0, tk.END)
        entry.insert(tk.END, str(math.log10(float(current))))
    elif text == "%":
        entry.delete(0, tk.END)
        entry.insert(tk.END, str(float(current) / 100))    

    else:
        entry.insert(tk.END, text)

root = tk.Tk()
root.title("Scientific Calculator")

# Apply a theme
style = ThemedStyle(root)
style.set_theme("radiance")  

entry = tk.Entry(root, width=30, font=('Arial', 20))
entry.grid(row=0, column=0, columnspan=4, padx=20, pady=20)

# Create a custom style for ttk buttons and set the font
custom_button_style = ttk.Style()
custom_button_style.configure("Custom.TButton", font=('Arial', 16))

buttons = [
    '7', '8', '9', '/',
    '4', '5', '6', '*',
    '1', '2', '3', '-',
    '0', 'C', '=', '+',
    '.', 'sqrt', 'x^2',"%" ,
    'sin', 'cos', 'tan', 'log'
]

row = 1
col = 0

for button in buttons:
    btn = ttk.Button(root, text=button, padding=20, style="Custom.TButton")
    btn.grid(row=row, column=col)
    btn.bind("<Button-1>", button_click)
    col += 1
    if col > 3:
        col = 0
        row += 1

root.mainloop()
