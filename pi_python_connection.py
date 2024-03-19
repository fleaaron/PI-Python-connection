import PIconnect as PI
from PIconnect.PIConsts import SummaryType, CalculationBasis, TimestampCalculation
from tqdm import tqdm
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


#Unique PI tag class

class PiTag:
    def __init__(self,tag,server_name, show_name = True):
        '''
        Unique class to store PI points
        
        '''
        with PI.PIServer(server=server_name) as server:
            self.point = server.search(str(tag))
        
        if show_name == True:
            print(self.point)
        else:
            pass


        self.current_value = self.point[0].current_value
        self.description   = self.point[0].description
        self.tag_name      = self.point[0].name
        self.uom           = self.point[0].units_of_measurement
        self.server_name   = server_name
        self.attributes    = self.point[0].raw_attributes
        self.last_update   = self.point[0].last_update


        #Time or event weighted averages
    def get_averaged_data(self,start, end, stepsize, calculation_mode = SummaryType.AVERAGE, averaging_mode= CalculationBasis.TIME_WEIGHTED):
        '''
        Correct input format of start and endtime is: 'YYYY-MM-DD'
                       and for stepsize could be: '%y' (years)
                                                  '%mo'(months)
                                                  '%d' (days)
                                                  '%h' (hours)
                                                  '%m' (minutes)   
        '''
        averaged_data = self.point[0].summaries(
                                        str(start),
                                        str(end),
                                        str(stepsize),
                                        summary_types= calculation_mode,
                                        calculation_basis= averaging_mode,
                                        time_type=TimestampCalculation.MOST_RECENT_TIME)
        
        data = pd.DataFrame(averaged_data)
        data.rename(columns={'AVERAGE' : self.tag_name}, inplace=True)
        
        for col in data.columns:
            data[col] = pd.to_numeric(data[col], errors= 'coerce')
         
        return data
    
    def get_compressed_data(self, start, end):
        compressed_data = self.point[0].recorded_values(
                                        str(start),
                                        str(end),
                                        boundary_type="inside")
        
        data = pd.DataFrame(compressed_data)

        
        return data

    
def get_date(data_frame):
        '''
        Creats a DateTime object from the time stamps of the input array
        '''
        date = data_frame.index
        return date

#Searching for PI tags based on the searching expression

def pi_scanner(tag):
    '''
    Searching for a specific PI tag or a collection of tags based on the expression given in the function argumentum, then returns a data frame with them
    '''
    with PI.PIServer(server='MOLSZHBPI') as server:
        points = server.search(str(tag))

        tags = np.array([])
        descriptions = np.array([])
        values = np.array([])
        uoms = np.array([])

        for pitag in points:
            tags         = np.append(pitag.name, tags)
            descriptions = np.append(pitag.description, descriptions)
            values       = np.append(pitag.current_value, values)
            uoms         = np.append(pitag.units_of_measurement, uoms)
        
        tags = tags.reshape(len(tags),1)
        descriptions = descriptions.reshape(len(descriptions),1)
        values = values.reshape(len(values),1)
        uoms = uoms.reshape(len(uoms),1)

        table = np.hstack((tags, descriptions, values, uoms))

        columns = np.array(['PI tag name', 'Description', 'Value', 'UoM'])

        table = pd.DataFrame(data = table, columns=columns)   
        
        return table



#Creates a table from multiple PI tags
def create_table(tags,start, end, stepsize):
    '''
    Creates a data frame from several PI tags
    tags: list or array of PiTag() objects
    Correct input format of start and endtime is: 'YYYY-MM-DD'
                       and for stepsize could be: '%y' (years)
                                                  '%mo'(months)
                                                  '%d' (days)
                                                  '%h' (hours)
                                                  '%m' (minutes)  
    '''
    names = [name.tag_name for name in tags]
    for i in range(len(tags)):
        if i ==0:
            frame = tags[i].get_averaged_data(start=start, end=end, stepsize=stepsize).values
        else:
            new_col = tags[i].get_averaged_data(start=start, end=end, stepsize=stepsize).values
            frame = np.hstack((frame, new_col))
    
    date = tags[0].get_averaged_data(start=start, end=end, stepsize=stepsize).index
    
    table = pd.DataFrame(frame, index = date, columns = names)

    #Dropping non numeric values
    for col in table.columns:
        table[col] = pd.to_numeric(table[col], errors= 'coerce')
    
    return table

#Extraction of lab data and the related tags

def create_lab_data_table(lab_data_tags, tag_names, start, end, averaging_time):


    #Reaching lab data tags
    print('~ Reaching laboratory data ~')
    lab_data_table = np.array([])

    for name in tqdm(lab_data_tags):
        try:
            lab_tag   = PiTag(tag=name, server_name='MOLSZHBPI')
            lab_data  = lab_tag.get_compressed_data(start=start, end=end)
            lab_data_values = np.float32(pd.DataFrame(lab_data).values)
        except TypeError:
            lab_data_values = pd.DataFrame(lab_tag.get_compressed_data(start=start, end=end)).values
        if len(lab_data_table) == 0:
            lab_data_table = lab_data_values
        else:
            lab_data_table = np.hstack((lab_data_table, lab_data_values))



    print('~ Laboratory data extraction is ready for the specified time range ~')
    print('~ PI data extraction is in progress ~')

    table = np.array([])
    row = np.array([])


    for date in tqdm(lab_data.index):
    
        if len(table) == 0 and len(row) != 0:
            table = np.append(table, row)
        elif len(table) == 0 and len(row) == 0:
            pass 
        else:
            table = np.vstack((table, row))

        row = np.array([])

        for name in tag_names:
            try:
                tag = PiTag(tag = name, server_name='MOLSZHBPI', show_name = False)
                values = tag.get_averaged_data(start=date-pd.Timedelta(hours = averaging_time), end=date, stepsize = str(averaging_time)+'h').values
            
                data = np.float16(values)
                    
            except TypeError:
                data = str(values)
                
            row = np.append(row, data)
        

    table = np.vstack((table, row))
    tag_names = np.append(tag_names, lab_data_tags)

    data = np.hstack((table, lab_data_table))
    date = lab_data.index
    df = pd.DataFrame(data, index= date.strftime("%Y/%m/%d, %H:%m"), columns = tag_names)

    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors= 'coerce')
    
    #df.dropna(inplace=True)

    print(' ~ PI data extraction is ready ~')

    return df
