import subprocess
from matplotlib import pyplot as plt


def run_shell(command, args):
    try:
        result = subprocess.run(command + args, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print("return from shell", result.stdout)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print("error from shell：", e.stderr)


def draw_dot(x, y, labels, items, subdir):
    plt.figure(figsize=(8, 6))
    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)
    plt.xlabel(labels["x"], fontsize=20)
    plt.ylabel(labels["y"], fontsize=20)
    plt.title(labels["title"], fontsize=20)
    colors = {"True": "black", "Che": "red"}
    markers={"True": "x", "Che": "+"}
    for item in items:
        if item == "CacheMissEqn":
            plt.plot(x[item], y[item], label = item, linewidth = 2, color='black')
        else:
            print(item)
            print(x["True"])
            print(y[item])
            plt.scatter(x["True"], y[item], label = item, s=120, marker=markers[item], color=colors[item])
    plt.ylim(top=1.1)
    # plt.xlim(left=0)
    plt.grid()
    plt.legend(fontsize=18)
    plt.savefig(subdir + "/figure.pdf")
    plt.show()
    run_shell(["cp", "-r"], [subdir, "/home/drg/results/"])
