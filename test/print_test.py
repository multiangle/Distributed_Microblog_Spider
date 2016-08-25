import time

class printer():
    def gen_timestr(self):
        tstr = time.strftime('%Y/%m/%d %H:%M:%S',time.localtime(time.time()))
        return tstr

    def gen_center_str(self, content, len=42, frame="|||"):
        if type(content)==str:
            content = content.split("\n")
            # content = [content]
        ret = ""
        for s in content:
            left = len-frame.__len__()*2-s.__len__()
            margin_left = left>>1
            margin_right = left-margin_left
            line = "{fr}{ml}{s}{mr}{fr}".format(
                ml = " "*margin_left,
                s = s,
                mr = " "*margin_right,
                fr = frame
            )
            ret += line+'\n'
        return ret

    def gen_block(self, content, len=42, frame="|||"):
        ret = "="*len + '\n'
        ret += self.gen_center_str(content,len,frame=frame)
        ret += "="*len + '\n'
        return ret


    def gen_block_with_time(self, content, len=42, frame="|||"):
        ret = "="*len+'\n'
        time_s = self.gen_timestr()
        timeline = "TIME: "+time_s
        ret += self.gen_center_str(timeline,len,frame=frame)
        return ret+self.gen_block(content,len,frame=frame)

p = printer()
print(p.gen_block_with_time("TIME TO GO\nhehehehe"))
