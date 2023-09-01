import sys

#execute using cmd e.g. python main movies update
#execute using cmd e.g. python main movies latest
#execute using cmd e.g. python main movies bulk
#
try:
    full_cmd_arguments = sys.argv
    user_input = full_cmd_arguments[1]
except:
    user_input = input("Please enter the filename")


print(user_input)
