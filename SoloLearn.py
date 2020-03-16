class SoloLearn(object):
    @staticmethod
    def group_by_values(dictionary):
        output = {}
        for keys, values in dictionary.items():
            if values in output.keys():
                output[values].append(keys)
            else:
                output[values] = [keys]
        return output


if __name__ == '__main__':
    SL = SoloLearn()
    files = {
     'Input.txt': 'Randy',
     'Code.py': 'Stan',
     'Output.txt': 'Randy',
     'test2': 'Randy',
     'test4': 'Randy',
     'test3': 'Stan',
    }
    print(SL.group_by_values(files))
