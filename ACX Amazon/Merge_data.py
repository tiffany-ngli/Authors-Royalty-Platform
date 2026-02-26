import pandas as pd
import os
import glob
import warnings


warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

def process_acx_file(file_path):
    try:
        xls = pd.ExcelFile(file_path)
        sheet_names = xls.sheet_names
        
        template_cols = [
            'Title', 'ASIN', 'Series', "Book's Order in Series", 'Series Name', 
            'Paperbacks', 'Distribution', 'Hardcover', 'Ebooks (Paid)', 'Ebooks (Free)', 
            'Audiobooks', 'Ad Orders', 'Total Ad Clicks', 'Ad Clicks (AMZ)', 'Ad Clicks (FB)', 
            'Reads', 'Gross Royalties ($)', 'Total Spending ($)', 'Spending (AMZ) ($)', 
            'Spending (FB) ($)', 'Spending (BookBub) ($)', 'Spending (External) ($)', 'Net Royalties ($)'
        ]
        
        df_extracted = pd.DataFrame(columns=template_cols)
        
        
        # FORMAT 3: The Current Era (2025)
        
        if 'Sales Detail (Net Sales)' in sheet_names:
            df = pd.read_excel(xls, sheet_name='Sales Detail (Net Sales)')
            
            df_extracted['Title'] = df['Title']
            df_extracted['ASIN'] = df['Product ID'] if 'Product ID' in df.columns else None
            df_extracted['Audiobooks'] = df['Net Units']
            df_extracted['Gross Royalties ($)'] = df['Net Sales']
            df_extracted['Net Royalties ($)'] = df['Net Royalties Earned']
            
        
        # FORMAT 1 & 2: The Early/Middle Era (2016 - 2022)
        
        elif 'Sales Details' in sheet_names:
            df = pd.read_excel(xls, sheet_name='Sales Details', skiprows=3)
            df = df.dropna(subset=['Title'])
            
            qty_cols = [col for col in df.columns if 'Qty' in str(col)]
            net_sales_cols = [col for col in df.columns if 'Net Sales' in str(col)]
            royalty_cols = [col for col in df.columns if 'Royalty' in str(col) and 'Earned' in str(col)]
            
            df_extracted['Title'] = df['Title']
            df_extracted['ASIN'] = df['Product ID'] if 'Product ID' in df.columns else None
            df_extracted['Audiobooks'] = df[qty_cols[-1]] if qty_cols else 0
            df_extracted['Gross Royalties ($)'] = df[net_sales_cols[-1]] if net_sales_cols else 0
            df_extracted['Net Royalties ($)'] = df[royalty_cols[-1]] if royalty_cols else 0
            
        else:
            return None

        
        # CLEANING & STANDARDIZING
        
        for col in ['Gross Royalties ($)', 'Net Royalties ($)']:
            if df_extracted[col].dtype == object:
                df_extracted[col] = df_extracted[col].astype(str).replace(r'[\$,]', '', regex=True)
            df_extracted[col] = pd.to_numeric(df_extracted[col], errors='coerce').fillna(0)
            
        df_extracted['Audiobooks'] = pd.to_numeric(df_extracted['Audiobooks'], errors='coerce').fillna(0)
        
        zero_cols = [
            'Paperbacks', 'Hardcover', 'Ebooks (Paid)', 'Ebooks (Free)', 
            'Ad Orders', 'Total Ad Clicks', 'Ad Clicks (AMZ)', 'Ad Clicks (FB)', 
            'Reads', 'Total Spending ($)', 'Spending (AMZ) ($)', 
            'Spending (FB) ($)', 'Spending (BookBub) ($)', 'Spending (External) ($)'
        ]
        for col in zero_cols:
            df_extracted[col] = 0
            
        return df_extracted

    except Exception as e:
        print(f"Error processing {os.path.basename(file_path)}: {e}")
        return None


# AUTOMATICALLY PROCESS NESTED FOLDERS


folder_path = '/Users/trippy/Library/CloudStorage/OneDrive-montclair.edu/#Intern/data/acx/incoming/'
file_list = glob.glob(os.path.join(folder_path, '**/*.xlsx'), recursive=True)

print(f"Found {len(file_list)} files across all subfolders to process.")

all_dataframes = []

for file in file_list:
    processed_df = process_acx_file(file) 
    if processed_df is not None and not processed_df.empty:
        all_dataframes.append(processed_df)

if all_dataframes:
    master_df = pd.concat(all_dataframes, ignore_index=True)
    
    
    # MERGE DUPLICATES (GROUP BY TITLE)
    
    # Define how each column should be merged
    agg_dict = {
        'ASIN': 'first',                     
        'Series': 'first',                   
        "Book's Order in Series": 'first',   
        'Series Name': 'first',              
        'Distribution': 'first',             
        'Paperbacks': 'sum',                 
        'Hardcover': 'sum',                  
        'Ebooks (Paid)': 'sum',              
        'Ebooks (Free)': 'sum',              
        'Audiobooks': 'sum',                 
        'Ad Orders': 'sum',                  
        'Total Ad Clicks': 'sum',            
        'Ad Clicks (AMZ)': 'sum',            
        'Ad Clicks (FB)': 'sum',             
        'Reads': 'sum',                      
        'Gross Royalties ($)': 'sum',        # Combine all gross sales!
        'Total Spending ($)': 'sum',         
        'Spending (AMZ) ($)': 'sum',         
        'Spending (FB) ($)': 'sum',          
        'Spending (BookBub) ($)': 'sum',     
        'Spending (External) ($)': 'sum',    
        'Net Royalties ($)': 'sum'           # Combine all net royalties!
    }
    
    # Group by the 'Title' column
    master_df = master_df.groupby('Title', as_index=False).agg(agg_dict)
    
    # Ensure columns are back in the exact template order
    template_cols = [
        'Title', 'ASIN', 'Series', "Book's Order in Series", 'Series Name', 
        'Paperbacks', 'Distribution', 'Hardcover', 'Ebooks (Paid)', 'Ebooks (Free)', 
        'Audiobooks', 'Ad Orders', 'Total Ad Clicks', 'Ad Clicks (AMZ)', 'Ad Clicks (FB)', 
        'Reads', 'Gross Royalties ($)', 'Total Spending ($)', 'Spending (AMZ) ($)', 
        'Spending (FB) ($)', 'Spending (BookBub) ($)', 'Spending (External) ($)', 'Net Royalties ($)'
    ]
    master_df = master_df[template_cols]
    
    
    # ROUND AND SORT THE DATA
    
    master_df['Net Royalties ($)'] = master_df['Net Royalties ($)'].round(2)
    master_df['Gross Royalties ($)'] = master_df['Gross Royalties ($)'].round(2)

    master_df = master_df.sort_values(by='Net Royalties ($)', ascending=False)
    
    output_filename = "ACX_to_Amazon_Template.csv"
    master_df.to_csv(output_filename, index=False)
    print(f"\nSuccess! The duplicates have been merged and the template is saved as: {output_filename}")
    
    
    print(master_df[['Title', 'Audiobooks', 'Gross Royalties ($)', 'Net Royalties ($)']].head(5))
else:
    print("No data was extracted.")