import sqlite3

class Map_Publication():
    def __init__(self):
        self.conn=sqlite3.connect("journal_table.db",check_same_thread = False)

    def Sel_Pub(self,journal):
        cursorObj=self.conn.cursor()
        journal_lis=journal.strip().split(",")
        # print(journal_lis)
        result={}
        for i in journal_lis:
            
            journal_std=i.upper()
            print(journal_std)
            sel_sel='''
            select Journal,Publisher from publication where Journal_std="{}"'''.format(journal_std)
            res="None"
            try:
                res=cursorObj.execute(sel_sel).fetchall()
            except:
                pass
            result[i]=res
        cursorObj.close()
        return result

if __name__=="__main__":
    journal_list="Plant Signaling & Behavior,Cancer Cell International,Clinical Epigenetics,Horticulturae,Journal of Assisted Reproduction and Genetics,NPJ Parkinson's disease,Breast Cancer,Bioengineering,Current Research in Microbial Sciences,Research Square"
    # journal_lis="Reactions Weekly"
    map=Map_Publication()
    result=map.Sel_Pub(journal_list)
    print(result)