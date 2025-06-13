import pandas as pd
import matplotlib.pyplot as plt
import os  # Add this import at the top

filenames = ['figureACSVCSV']
# filenames = ['figureACSVCSV','figureACSVCSVOpt','figureBCSVPG','figureBCSVPGOpt']

output_mapping = {
    'figureACSVCSV': 'par_scale_csv_csv_write_1.pdf',
    # 'figureACSVCSVOpt': 'par_scale_csv_csv_ser_8.pdf',
    # 'figureBCSVPG': 'par_scale_postgres_csv_write_1.pdf',
    # 'figureBCSVPGOpt': 'par_scale_postgres_csv_read_8.pdf',
}

for filename in filenames:
    # Load CSV data
    csv_file_path = os.path.join('res', f'{filename}.csv')  # Updated path
    # csv_file_path = f'{filename}.csv'  # Replace with your actual file path
    df = pd.read_csv(csv_file_path)

    cols_to_consider = [
    'env', 'read_par', 'deser_par', 'comp_par',
    'send_par', 'rcv_par', 'decomp_par', 'ser_par',
    'write_par', 'table'
    ]
    
    # Remove duplicates based on these columns
    df = df.drop_duplicates(subset=cols_to_consider)
    
    # List of parallelism parameters
    parallelism_keys = [
        'read_par', 'deser_par', 'comp_par',
        'decomp_par', 'ser_par', 'write_par'
    ]
    
    # Define the fixed configuration (default values for all parameters)
    ser_par = 1
    read_par = 1
    # if "figureACSVCSVOpt" in filename:
    #     ser_par = 8
    # if "figureBCSVPGOpt" in filename:
    #     read_par = 8
    
    fixed_config = {
        'read_par': read_par,
        'deser_par': 1,
        'comp_par': 1,
        'decomp_par': 1,
        'ser_par': ser_par,
        'write_par': 1  # Example: This is the default write_par value
    }
    df = df.groupby(['env','table']+list(fixed_config.keys()) + ['time']).mean().reset_index()
    
    
    # Set up plot style
    plt.rcParams['text.usetex'] = True
    plt.rcParams['font.family'] = 'serif'
    plt.rcParams['font.serif'] = ['Computer Modern Roman']
    plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})
    
    # Set up the plot
    plt.figure(figsize=(6, 3.75))
    
    # Range of values to scale (1 to 16, doubling)
    scale_values = [1, 2, 4, 8, 16]
    
    # Plot each parameter
    for param in parallelism_keys:
        # Filter the dataset to match the fixed configuration
        filtered_df = df.copy()
    
        # Apply the fixed configuration for all parameters except the one being scaled
        for key, value in fixed_config.items():
            if key != param:  # Keep the current parameter free to scale
                filtered_df = filtered_df[filtered_df[key] == value]
    
        # Extract the data for the current parameter
        filtered_df = filtered_df[filtered_df[param].isin(scale_values)].sort_values(by=param)
        
        # Extract x (scaled parameter values) and y (time)
        x = filtered_df[param]
        y = filtered_df['time']
        
        # Plot the results
        plt.plot(x, y, marker='x', label=f"{param}")
    
    # Customize the plot
    plt.ylim(0)
    plt.xlabel('Parallelism Degree')
    plt.ylabel('Time (s)')
    plt.xticks(scale_values, labels=[str(val) for val in scale_values])  # Ensure xticks match the scaled values
    plt.legend(loc='best', ncol=2)
    
    # Add grid and improve layout
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(f'{output_mapping[filename]}', bbox_inches='tight')
    
    # Show the plot
    plt.show()
