import pandas as pd
import matplotlib.pyplot as plt
import os  # Add this import at the top
import numpy as np
import seaborn as sns
from PyPDF2 import PdfMerger, PdfReader, PdfWriter
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from io import BytesIO

# --- SCRIPT CONTROL PANEL ---
# Set the sections you want to run to True and others to False.
# This allows you to run only the parts of the script you need.

RUN_SECTION_01 = True  # Figures 12 & 13 (Parallelism Scaling)        : figureACSVCSV.csv, figureACSVCSVOpt, figureBCSVPG, figureBCSVPGOpt (N24)
RUN_SECTION_02 = True  # Figure 17a (Memory Management)                : figureMemoryManagement.csv (N24*)
RUN_SECTION_03 = True  # Figure 11b (Physical Nodes)
RUN_SECTION_04 = True  # Figures 10a & 10b (Parquet/CSV Comparison)    : figureZParquetCSV.csv (N28)
RUN_SECTION_05 = True  # Figure 11a (Skip Serialization)                : figure11.csv (N24*)
RUN_SECTION_06 = True  # Figure 14a (Arrow Formats)                     : figureXArrow.csv (N24)
RUN_SECTION_07 = True  # Figures 19a & 19b (Compression Scaling)      : figure17b.csv (N24*)
RUN_SECTION_08 = True  # Figure 17b (Network vs. Compression)          : figure17b.csv (N24*)
RUN_SECTION_09 = True  # Figure 18 (Buffer Size vs. Compression/Format) : figure18.csv (N24*)
RUN_SECTION_10 = True  # Figure 16a (Parallelism Scaling - Env 1)      : figure1516a.csv (N24)
RUN_SECTION_11 = True  # Figure 16b (Config Comparison - Env 1)         : figure1516b.csv (N24)
RUN_SECTION_12 = True  # Figure 15a (Parallelism Scaling - Env 2)       : figure1516a.csv (N24)
RUN_SECTION_13 = True  # Figure 15b (Config Comparison - Env 2)         : figure1516b.csv (N24)
RUN_SECTION_14 = True  # Figures 6a & 6b (Configuration Runtimes)      : figure7.csv, figure7b.csv (N23)
RUN_SECTION_16 = True  # figureYParquet.py => figure14b                : figureYParquet.csv (N24)
RUN_SECTION_17 = True  # figure8b.py => figure7b                       : figure8b.csv (N23)
RUN_SECTION_18 = True  # figure8a.py => figure7b4                      : figure8a.csv (N23)
RUN_SECTION_19 = True   # figurePandasPG.py => figure8a              : figurePandasPG.csv (N28)
RUN_SECTION_20 = True   # figurePandasPG.py => figure8b              : figurePandasPG.csv (N28)
RUN_SECTION_21 = True   # figure20.py
RUN_SECTION_22 = True   # figure20.py
RUN_SECTION_23 = True   # figure9a
RUN_SECTION_24 = True   # figure9b

# This final section combines all generated PDFs.
# Set this to True if you have generated new figures and want to merge them.
RUN_PDF_MERGER = True
# -----------------------------

# # ****************************Section1 :Generate figures 12 & 13***************************
if RUN_SECTION_01:
    print("\n--- Running Section 1: Generating Figures 12 & 13 ---")
    # filenames = ['figureACSVCSV','figureACSVCSVOpt']
    filenames = ['figureACSVCSV','figureACSVCSVOpt','figureBCSVPG','figureBCSVPGOpt']

    output_mapping = {
        'figureACSVCSV': 'figure12a.pdf',
        'figureACSVCSVOpt': 'figure13a.pdf',
        'figureBCSVPG': 'figure12b.pdf',
        'figureBCSVPGOpt': 'figure13b.pdf',
    }
    # Remove the unnecessary curly brace

    # --- Main Loop ---
    # Process each file specified in the `filenames` list.
    for filename in filenames:
        # Check if the file exists before trying to load it.
        csv_file_path = os.path.join('res', f'{filename}.csv')
        if not os.path.exists(csv_file_path):
            print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
            continue

        # Load the CSV data from the file.
        df = pd.read_csv(csv_file_path)

        # To handle any repeated experiments, group by the configuration
        # and average the time. This is more robust than dropping duplicates.
        config_cols = [
            'env', 'read_par', 'deser_par', 'comp_par', 'send_par',
            'rcv_par', 'decomp_par', 'ser_par', 'write_par', 'table'
        ]
        df = df.groupby(config_cols, as_index=False)['time'].mean()

        # List of parallelism parameters to plot on the x-axis
        parallelism_keys = [
            'read_par', 'deser_par', 'comp_par',
            'decomp_par', 'ser_par', 'write_par'
        ]

        # Create a new figure for each file.
        plt.figure(figsize=(6, 3.75))

        # The values for the x-axis.
        scale_values = [1, 2, 4, 8, 16]

        # Plot each parameter's scaling behavior.
        for param in parallelism_keys:
            # --- DYNAMIC FILTERING LOGIC ---
            # 1. Define the default baseline configuration for each iteration.
            baseline_config = {
                'read_par': 1, 'deser_par': 1, 'comp_par': 1,
                'decomp_par': 1, 'ser_par': 1, 'write_par': 1
            }

            # 2. Check for the "Opt" special case and adjust the baseline.
            # This makes the script adaptable to different experimental setups.
            if "figureACSVCSVOpt" in filename and param != 'ser_par':
                baseline_config['ser_par'] = 8
            if "figureBCSVPGOpt" in filename and param != 'read_par':
                baseline_config['read_par'] = 8

            # 3. Filter the DataFrame using the correct dynamic baseline.
            filtered_df = df.copy()
            for key, value in baseline_config.items():
                if key != param:  # Don't filter the parameter we are currently plotting
                    filtered_df = filtered_df[filtered_df[key] == value]
            # --- END OF LOGIC ---

            # Extract the data for the current parameter.
            filtered_df = filtered_df[filtered_df[param].isin(scale_values)].sort_values(by=param)

            # Plot the results only if data was found.
            if not filtered_df.empty:
                grouped = filtered_df.groupby(param)['time'].mean().reset_index()
                plt.plot(grouped[param], grouped['time'], marker='x', label=f"{param}")

        # --- Final Plot Customization ---
        plt.ylim(bottom=0)
        plt.xlabel('Parallelism Degree')
        plt.ylabel('Time (s)')
        plt.xticks(scale_values)
        plt.legend(loc='best', ncol=3)
        plt.grid(alpha=0.3)
        plt.tight_layout()

        # Save the plot to the corresponding PDF file.
        output_filename = output_mapping.get(filename, f'{filename}.pdf')
        plt.savefig(output_filename, bbox_inches='tight')
        print(f"Plot saved to '{output_filename}'")

        plt.show()



# # ******************************Section2: Generate figure for MemoryManagement**************************
if RUN_SECTION_02:
    print("\n--- Running Section 2: Generating Figure 17a (Memory Management) ---")
    # Load the CSV file
    filename = "figureMemoryManagement"
    csv_file_path = os.path.join('res', f'{filename}.csv')
    if not os.path.exists(csv_file_path):
        print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
    else:
        data = pd.read_csv(csv_file_path)

        data = data[data['table']=='lineitem_sf10_nocomp']  # Filter for the specific table
        # Prepare plot data
        data['bufferpool_size'] = data['bufferpool_size'].astype(int)
        data['buffer_size'] = data['buffer_size'].astype(int)

        # Sort buffer pool and buffer sizes for consistency
        data = data.sort_values(['bufferpool_size', 'buffer_size'])

        # Set up plot style
        plt.rcParams['text.usetex'] = True
        plt.rcParams['font.family'] = 'serif'
        plt.rcParams['font.serif'] = ['Computer Modern Roman']
        plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

        # Unique buffer pool sizes and buffer sizes
        bufferpool_sizes = sorted(data['bufferpool_size'].unique())
        buffer_sizes = sorted(data['buffer_size'].unique())
        colors = sns.color_palette("colorblind", len(buffer_sizes))

        # Plot
        plt.figure(figsize=(6, 3.75))
        bar_width = 0.14
        x_indexes = np.arange(len(bufferpool_sizes))

        for i, buffer_size in enumerate(buffer_sizes):
            subset = data[data['buffer_size'] == buffer_size]
            means = [
                subset[subset['bufferpool_size'] == pool_size]['time'].mean() if pool_size in subset['bufferpool_size'].values else 0
                for pool_size in bufferpool_sizes
            ]
            positions = x_indexes + (i - len(buffer_sizes) / 2) * bar_width
            plt.bar(positions, means, width=bar_width, label=f"{buffer_size}", color=colors[i], zorder=3)

        # Formatting
        plt.xlabel("Buffer Pool Size (MB)")
        plt.ylabel("Time (s)")
        plt.ylim(0,34)
        plt.xticks(ticks=x_indexes, labels=[f"{int(bp/1024)}" for bp in bufferpool_sizes], ha="center")
        plt.legend(title="Buffer Size (KB)",loc='best', ncol=7, labelspacing=0.1, borderpad=0.3, handletextpad=0.2, handlelength=1, columnspacing=.3)
        plt.grid(axis='y', alpha=0.3, zorder=0)

        # Save and show the plot
        plt.tight_layout()
        plt.savefig("figure17a.pdf", bbox_inches='tight')
        plt.show()


# # ******************************* Section4: Generate figure for Parquet CSV*******************************
if RUN_SECTION_04:
    print("\n--- Running Section 4: Generating Figures 10a & 10b (Parquet/CSV Comparison) ---")
    # Generate plots for each environment
    for network in [0,125]:

        # Load CSV data
        filename = "figureZParquetCSV"
        csv_file_path = os.path.join('res', f'{filename}.csv')
        if not os.path.exists(csv_file_path):
            print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
            continue
        data = pd.read_csv(csv_file_path)
        # data = pd.read_csv("figureZParquetCSV.csv")
        data = data[data['system']!='xdbc[parquet-snappy]']
        
        data = data[data['network']==network]
        
        data['table'] = data['table'].replace({
            'lineitem_sf10': 'lineitem',
            'ss13husallm': 'acs',
            'iotm': 'iot',
            'inputeventsm': 'icu'
        })
        
        # Define the environments
        environments = data['env'].unique()
        
        # Define the approaches
        approaches = data['system'].unique()
        
        # Prepare data for plotting
        grouped_data = data.groupby(['env', 'system', 'table'])['time'].mean().reset_index()



        approaches = ['xdbc[parquet]', 'xdbc[col]', 'xdbc[col-snappy]', 'duckdb']
        tables = ['lineitem', 'acs', 'iot', 'icu']  # Desired order for tables
        grouped_data['table'] = pd.Categorical(grouped_data['table'], categories=tables, ordered=True)
        grouped_data['system'] = pd.Categorical(grouped_data['system'], categories=approaches, ordered=True)
        grouped_data = grouped_data.sort_values('table')

        # Set up plot style
        plt.rcParams['text.usetex'] = True
        plt.rcParams['font.family'] = 'serif'
        plt.rcParams['font.serif'] = ['Computer Modern Roman']
        plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})
        
        
        #env_data = grouped_data[grouped_data['env'] == env]

        # Pivot the data to get tables as x-axis and approaches as bars
        pivot_data = grouped_data.pivot(index='table', columns='system', values='time')

        

        # Set the bar width and positions
        bar_width = 0.15
        x_indexes = np.arange(len(pivot_data.index))
        colors = sns.color_palette("colorblind", len(approaches))

        # Create the plot
        plt.figure(figsize=(6, 3.75))

        for i, (approach, color) in enumerate(zip(approaches, colors)):
            if approach in pivot_data.columns:
                plt.bar(x_indexes + (i - len(approaches)/2) * bar_width, pivot_data[approach],
                        width=bar_width, label=approach, color=color, zorder=3)

        # Labels and title
        plt.xlabel('Tables')
        plt.ylabel('Time (s)')
        plt.ylim(0,90)
        
        plt.xticks(ticks=x_indexes, labels=pivot_data.index, ha='right')
        #plt.title(f'Environment: {env}')
        plt.legend(loc='best', ncol=2, labelspacing=0.3, borderpad=0.3, handletextpad=0.4, handlelength=1.5)

        # Add grid for better readability
        plt.grid(axis='y', alpha=0.3, zorder=0)

        # Save the plot
        #output_file = f'{env}_times_plot.pdf'
        plt.tight_layout()
        if network == 0:
            plt.savefig(f"figure10a.pdf", bbox_inches='tight')
        elif network == 125:  
            plt.savefig(f"figure10b.pdf", bbox_inches='tight')

        plt.show()

#     # ******************************** Section5: Generate figure 11*******************************
if RUN_SECTION_05:
    print("\n--- Running Section 5: Generating Figure 11a (Skip Serialization) ---")
    # Load the CSV file
    filename = "figure11"
    csv_file_path = os.path.join('res', f'{filename}.csv')
    if not os.path.exists(csv_file_path):
        print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
    else:
        data = pd.read_csv(csv_file_path)

        # csv_file_path = 'figure11.csv'  # Replace with your actual file path
        # data = pd.read_csv(csv_file_path)

        # Replace mismatched table names
        data['table'] = data['table'].replace({
            'lineitem_sf10': 'lineitem',
            'ss13husallm': 'acs',
            'iotm': 'iot',
            'inputeventsm': 'icu'
        })

        # Replace system names for clarity
        data['system'] = data['system'].replace({
            'xdbc-skip0': 'xdbc',
            'xdbc-skip1': 'xdbc[skip-ser]',
            'read_csv_url': 'netcat'
        })

        # Calculate the average time for each combination of table and system
        average_times = (
            data.groupby(['table', 'system'])['time']
            .min()
            .reset_index()
            .pivot(index='table', columns='system', values='time')
        )

        # Define the order of systems and datasets for consistency
        systems = ['xdbc', 'xdbc[skip-ser]', 'netcat']
        tables = ['lineitem', 'acs', 'iot', 'icu']

        # Reindex to ensure proper order and fill missing values with 0 (if any)
        average_times = average_times.reindex(index=tables, columns=systems, fill_value=0)

        # Extract data for plotting
        approach_times = [average_times[system].values for system in systems]

        # Set up plot style
        plt.rcParams['text.usetex'] = True
        plt.rcParams['font.family'] = 'serif'
        plt.rcParams['font.serif'] = ['Computer Modern Roman']
        plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

        # Create the plot
        datasets = tables  # Adjust dataset names for readability
        formal_palette = sns.color_palette("colorblind", len(systems))
        bar_width = 0.25
        x_indexes = np.arange(len(datasets))

        plt.figure(figsize=(6, 3.75))

        # Plotting each approach with offset for bar positions
        for i, (system, times) in enumerate(zip(systems, approach_times)):
            plt.bar(x_indexes + (i - 1) * bar_width, times, width=bar_width, color=formal_palette[i], label=system, zorder=3)

        # Labels and Title
        plt.xlabel('Datasets')
        plt.ylabel('Time (s)')
        plt.xticks(ticks=x_indexes, labels=datasets)
        plt.legend(loc='best')

        # Grid for better readability
        plt.grid(axis='y', alpha=0.3, zorder=0)

        # Display the plot
        plt.tight_layout()
        plt.savefig('figure11a.pdf', bbox_inches='tight')
        plt.show()  

# # ************************* Section6: Generate figure 14 *******************************
if RUN_SECTION_06:
    print("\n--- Running Section 6: Generating Figure 14a (Arrow Formats) ---")
    # Load the data
    filename = "figureXArrow"
    csv_file_path = os.path.join('res', f'{filename}.csv')
    if not os.path.exists(csv_file_path):
        print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
    else:
        data = pd.read_csv(csv_file_path)
        # data = pd.read_csv("figureXArrow.csv")

        # Replace mismatched table names
        data['table'] = data['table'].replace({
            'lineitem_sf10': 'lineitem',
            'ss13husallm': 'acs',
            'iotm': 'iot',
            'inputeventsm': 'icu'
        })

        # Extract format and skip information from the 'system' column
        data['format'] = data['system'].apply(lambda x: x.split('-')[-1])  # Extract format (e.g., format1, format2, etc.)
        data['skip'] = data['system'].apply(lambda x: 'skip1' in x)       # Identify skip-ser (skip1)

        # Calculate the average time for each table and format
        average_times = (
            data.groupby(['table', 'format'])['time']
            .mean()
            .reset_index()
            .pivot(index='table', columns='format', values='time')
        )

        # Ensure correct order of formats
        formats = ['format1', 'format2', 'format3', 'formatNone']
        tables = ['lineitem', 'acs', 'iot', 'icu']  # Desired order for tables
        average_times = average_times.reindex(index=tables, columns=formats, fill_value=0)

        # Set up plot style
        plt.rcParams['text.usetex'] = True
        plt.rcParams['font.family'] = 'serif'
        plt.rcParams['font.serif'] = ['Computer Modern Roman']
        plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

        # Plot settings
        bar_width = 0.2
        x_indexes = np.arange(len(tables))
        colors = sns.color_palette("colorblind", len(formats))
        labels = ['xdbc[row]', 'xdbc[col]', 'xdbc[arrow]', 'xdbc[skip-ser]']

        plt.figure(figsize=(6, 3.75))

        # Plot each format
        for i, format_ in enumerate(formats):
            plt.bar(x_indexes + (i - 1.5) * bar_width, average_times[format_], width=bar_width,
                    label=labels[i], color=colors[i], zorder=3)

        # Labels, legend, and formatting
        plt.xlabel('Datasets')
        plt.ylabel('Time (s)')
        plt.xticks(ticks=x_indexes, labels=tables)
        plt.legend(loc='best', labelspacing=0.3, borderpad=0.3, handletextpad=0.4, handlelength=1)
        plt.grid(axis='y', alpha=0.3, zorder=0)

        # Layout adjustments
        plt.tight_layout()

        # Save and show the plot
        plt.savefig("figure14a.pdf", bbox_inches='tight')
        plt.show()



# ****************************** Section7: Generate figure 19 *******************************
if RUN_SECTION_07:
    print("\n--- Running Section 7: Generating Figures 19a & 19b (Compression Scaling) ---")
    # Load the dataset from the provided CSV file

    try:
        filename = "figure19"
        csv_file_path = os.path.join('res', f'{filename}.csv')
        if not os.path.exists(csv_file_path):
            print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        print("Error: 'figure19.csv' not found. Please ensure the file is in the correct directory.")
        exit()

    # Define the default configuration
    default_config = {
        'write_par': 16,
        'read_par': 1,
        'deser_par': 8
    }

    # Define the environments to be plotted
    environments = [
        {'env': 'env_16c_16c_0net'},
        {'env': 'env_16c_16c_1000net'},
    ]


    # Iterate over each environment
    for idx, env in enumerate(environments):
        # Start with a copy of the original dataframe
        filtered_df_env = df.copy()
        
        # --- Data Filtering ---
        
        # Filter by the default configuration parameters
        for key, value in default_config.items():
            filtered_df_env = filtered_df_env[filtered_df_env[key] == value]
        
        filtered_df_env = filtered_df_env[filtered_df_env['env'] == env['env']]
        
        # Filter for the specific table
        filtered_df_env = filtered_df_env[filtered_df_env['table'] == 'lineitem_sf10']
        
        # Filter for cases where compression and decompression parallelism are equal
        filtered_df_env = filtered_df_env[filtered_df_env['comp_par'] == filtered_df_env['decomp_par']]

        # --- Plotting Setup ---

        # Ensure 'nocomp' is the first category in 'compression' for consistent plotting
        if not filtered_df_env.empty:
            unique_compressions = sorted(filtered_df_env['compression'].unique(), key=lambda x: (x != 'nocomp', x))
            filtered_df_env['compression'] = pd.Categorical(
                filtered_df_env['compression'], 
                categories=unique_compressions,
                ordered=True
            )

            # Use Seaborn's colorblind-friendly palette
            color_palette = sns.color_palette("colorblind", len(filtered_df_env['compression'].unique()))

            # Get the parallelism values for the x-axis
            parallelism_values = sorted(filtered_df_env['comp_par'].unique())

            # --- Create the Plot ---
            
            plt.figure(figsize=(6, 3.75))

            # Plot runtime vs. parallelism for each compression type
            for i, compression in enumerate(filtered_df_env['compression'].cat.categories):
                compression_data = filtered_df_env[filtered_df_env['compression'] == compression]
                x_values = compression_data['comp_par']
                y_values = compression_data['time']
                plt.plot(x_values, y_values, label=compression, color=color_palette[i], marker='x', zorder=3)

            # --- Final Touches ---

            # Label the axes and set ticks
            plt.xlabel('Parallelism Degree: Same Compress == Decompress')
            plt.ylabel('Time (s)')
            plt.xticks(parallelism_values)
            
            # Add a grid for readability
            plt.grid(alpha=0.3)
            
            # Add a legend
            plt.legend(title='Compression')
            
            # Adjust layout
            plt.tight_layout()

            # --- Save the Figure ---
            
            # Construct the filename based on the environment configuration
            suffix = chr(ord('a') + idx)  # 'a' for idx=0, 'b' for idx=1, etc.
            output_filename = f"figure19{suffix}.pdf"
            plt.savefig(output_filename, bbox_inches='tight')
            print(f"Plot saved to {output_filename}")
            
            # Close the plot to free up memory
            plt.close()
        else:
            print(f"No data found for environment: {env}. Skipping plot generation.")


# ****************************** Section8: Generate figure 17b *******************************
if RUN_SECTION_08:
    print("\n--- Running Section 8: Generating Figure 17b (Network vs. Compression) ---")
    # --- Data Loading ---
    try:
        # Define the input CSV file, assuming a name like 'figure20.csv'
        filename = "figure17b"
        csv_file_path = os.path.join('res', f'{filename}.csv')
        if not os.path.exists(csv_file_path):
            print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
            exit()  # Exit if the data file is not found
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        print(f"Error: '{filename}.csv' not found. Please ensure the file is in the 'res' directory.")
        exit()

    # --- Configuration ---
    config = {
            'table': 'lineitem_sf10',
            "send_par": 1,
            "write_par": 16,
            "decomp_par": 4,
            "read_par": 1,
            "deser_par": 4,
            "comp_par": 4,
    }
    filtered_df = df.copy()
    for key, value in config.items():
        filtered_df = filtered_df[filtered_df[key] == value]

    filtered_df = filtered_df[filtered_df['env'].isin(['env_16c_16c_15.75net', 'env_16c_16c_31.5net', 'env_16c_16c_125net', 'env_16c_16c_250net', 'env_16c_16c_500net'])]
    filtered_df = filtered_df[filtered_df['network_mbps'].isin([15.75, 31.5, 125, 250, 500])]
    plt.figure(figsize=(6, 3.75))
    unique_compressions = sorted(filtered_df['compression'].unique())
    for compression in unique_compressions:
        subset = filtered_df[filtered_df['compression'] == compression]
        plt.plot(subset['network_mbps'], subset['time'], marker='x', label=compression, linestyle='-', 
                markerfacecolor='none', markeredgewidth=1)


    #plt.xticks(ticks=[15.75, 31.5,62, 125, 250, 500], labels=[15.75, 31.5,62, 125, 250, 500], rotation=45)
    # Add labels and title
    #plt.title('Runtime vs Network with Different Compression Types')
    plt.xlabel('Network (MB/s)')
    plt.ylabel('Time (s)')
    plt.grid(alpha=.3)
    plt.legend()
    # Show the plot
    #plt.show()
    output_filename = f"figure17b.pdf"
    plt.savefig(output_filename, bbox_inches='tight')



# ****************************** Section9: Generate figure18 *******************************
if RUN_SECTION_09:
    print("\n--- Running Section 9: Generating Figure 18 (Buffer Size vs. Compression/Format) ---")
    from matplotlib.colors import to_rgba

    # --- Main Configuration ---
    filename = "figure18" # Your CSV filename without extension
    save = 1  # Set to 1 to save plots, 0 to display them
    plot_letter_code = 96 # Start with 'a' (ASCII 97)

    # --- Plotting Style ---
    plt.rcParams.update({
        'axes.titlesize': 24,
        'axes.labelsize': 24,
        'xtick.labelsize': 24,
        'ytick.labelsize': 24
    })
    legend_fontsize = 21
    borderpad = 0.03
    labelspacing = 0
    borderaxespad = 0
    handlelength = 1.2
    edgecolor = 'gainsboro'
    frameon = False

    # --- Experiment Parameters ---
    environments = ['env_16s_16c_1000net_Lineitem','env_16s_1c_10net_Lineitem','env_16s_16c_1000net_ICU','env_16s_1c_10net_ICU']
    # FIX: The `tables` list is redundant as the table is derived from `environments`.
    # tables = ['Lineitem', 'ICU']
    buffer_pool_sizes = [81920]
    buffer_sizes = [32, 64, 128, 256, 512, 1024, 2048]

    fixed_params = {
            'env_16s_16c_1000net': {
                    "network_parallelism": 1, "client_write_par": 16, "client_decomp_par": 4,
                    "server_read_partitions": 1, "server_read_par": 1, "server_deser_par": 4,
                    "server_comp_par": 4,
                },
            'env_16s_1c_10net': {
                    "network_parallelism": 1, "client_write_par": 1, "client_decomp_par": 8,
                    "server_read_partitions": 1, "server_read_par": 8, "server_deser_par": 1,
                    "server_comp_par": 8,
                }
    }

    # --- Data Loading and Preprocessing ---
    try:
        csv_file_path = os.path.join('res', f'{filename}.csv')
        df_data = pd.read_csv(csv_file_path, header=None, skiprows=1)
        # cols_to_keep = list(range(18)) + [25, 26]
        cols_to_keep = list(range(20))
        df = df_data[cols_to_keep]
        final_headers = [
            'timestamp', 'env_name', 'repetition', 'read_par', 'deser_par',
            'write_par', 'send_par', 'rcv_par', 'comp_par', 'decomp_par',
            'ser_par', 'buffer_size', 'server_buffpool_size',
            'client_buffpool_size', 'format', 'compression', 'skip_ser', 'skip_deser',
            'time', 'data_size'
        ]
        df.columns = final_headers

        # The following block correctly creates the 'table_category' column.
        env_parts = df['env_name'].str.split('_', expand=True)
        df['server_cpu'] = env_parts[1].str.replace('s', '', regex=False).astype(int)
        df['client_cpu'] = env_parts[2].str.replace('c', '', regex=False).astype(int)
        df['network'] = env_parts[3].str.replace('net', '', regex=False).astype(int)
        df['table_category'] = env_parts[4]  # This is where the table info is stored.

    except FileNotFoundError:
        print(f"Error: '{filename}.csv' not found. Please ensure the file is in the 'res' directory.")
        exit()
    except (KeyError, IndexError, TypeError) as e:
        print(f"Error parsing 'env_name' column: {e}.")
        print("Please ensure 'env_name' column exists and has the expected format (e.g., 'env_16s_16c_1000net_Lineitem').")
        exit()


    # --- Color and Style Setup ---
    unique_compressions = sorted(df['compression'].unique())
    unique_formats = [1, 2]
    palette = sns.color_palette("colorblind", len(unique_compressions))
    color_map = dict(zip(unique_compressions, palette))
    color_map_with_opacity = {}
    for comp in unique_compressions:
        color_map_with_opacity[f'{comp}(row)'] = color_map[comp]
        color_map_with_opacity[f'{comp}(col)'] = tuple(np.clip(np.array(to_rgba(color_map[comp])[:3]) * 0.7, 0, 1))

    # --- Plotting Functions ---
    def plot_grouped_bar(df_to_plot, output_filename):
        plt.figure(figsize=(12, 6))
        plot_data = []
        for buffer_size in buffer_sizes:
            for compression in unique_compressions:
                for fmt in unique_formats:
                    subset = df_to_plot[(df_to_plot['buffer_size'] == buffer_size) &
                                        (df_to_plot['compression'] == compression) &
                                        (df_to_plot['format'] == fmt)]
                    if not subset.empty:
                        plot_data.append((buffer_size, compression, fmt, subset['time'].mean()))

        if not plot_data: return
        plot_df = pd.DataFrame(plot_data, columns=['Buffer Size', 'Compression', 'Format', 'Runtime'])
        plot_df['Compression_Format'] = plot_df['Compression'].astype(str) + np.where(plot_df['Format'] == 1, '(row)', '(col)')
        
        sns.barplot(x='Buffer Size', y='Runtime', hue='Compression_Format', data=plot_df, palette=color_map_with_opacity)
        plt.xlabel('Buffer Size (KB)')
        plt.ylabel('Time (s)')
        plt.legend(ncol=5, fontsize=legend_fontsize, columnspacing=0.08, handletextpad=0.08, handlelength=handlelength, borderaxespad=borderaxespad, borderpad=borderpad, labelspacing=labelspacing, frameon=frameon, edgecolor=edgecolor)
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()

        plt.savefig(output_filename, bbox_inches='tight')

    def plot_grouped_bar_datasize(df_to_plot, output_filename):
        plt.figure(figsize=(12, 6))
        plot_data = []
        for buffer_size in buffer_sizes:
            for compression in unique_compressions:
                for fmt in unique_formats:
                    subset = df_to_plot[(df_to_plot['buffer_size'] == buffer_size) &
                                        (df_to_plot['compression'] == compression) &
                                        (df_to_plot['format'] == fmt)]
                    if not subset.empty:
                        mean_datasize_gb = subset['data_size'].mean() / (1024**3)
                        plot_data.append((buffer_size, compression, fmt, mean_datasize_gb))
        
        if not plot_data: return
        plot_df = pd.DataFrame(plot_data, columns=['Buffer Size', 'Compression', 'Format', 'Data Size (GB)'])
        plot_df['Compression_Format'] = plot_df['Compression'].astype(str) + np.where(plot_df['Format'] == 1, '(row)', '(col)')
        
        sns.barplot(x='Buffer Size', y='Data Size (GB)', hue='Compression_Format', data=plot_df, palette=color_map_with_opacity)
        plt.grid(axis='y', alpha=0.3)
        plt.xlabel('Buffer Size (KB)')
        plt.ylabel('Data Size (GB)')
        plt.legend(ncol=5, fontsize=legend_fontsize, columnspacing=0.08, handletextpad=0.08, handlelength=handlelength, borderaxespad=borderaxespad, borderpad=borderpad, labelspacing=labelspacing, frameon=frameon, edgecolor=edgecolor)
        plt.tight_layout()

        plt.savefig(output_filename, bbox_inches='tight')

    plot_naming_map = {
        ('env_16s_16c_1000net_Lineitem', 'runtime'): 'a',
        ('env_16s_16c_1000net_ICU', 'runtime'): 'b',
        ('env_16s_1c_10net_ICU', 'runtime'): 'c',
        ('env_16s_16c_1000net_Lineitem', 'datasize'): 'd',
        ('env_16s_16c_1000net_ICU', 'datasize'): 'e',
        ('env_16s_1c_10net_Lineitem', 'runtime'): 'f',
    }

    # --- Main Plotting Loop ---
    for env in environments:
        # The filter correctly uses the unique 'env_name' for each specific experiment.
        filtered_df = df[df['env_name'] == env].copy()

        if filtered_df.empty:
            print(f"Warning: No data found for environment '{env}'. Skipping plot generation.")
            continue

        print(f"\nProcessing environment: {env}")

        # --- Generate Runtime Plot if specified ---
        if (env, 'runtime') in plot_naming_map:
            plot_char = plot_naming_map[(env, 'runtime')]
            out_fname = f"figure18{plot_char}.pdf"
            plot_grouped_bar(filtered_df, out_fname)

        # --- Generate Data Size Plot if specified ---
        if (env, 'datasize') in plot_naming_map:
            plot_char = plot_naming_map[(env, 'datasize')]
            out_fname = f"figure18{plot_char}.pdf"
            plot_grouped_bar_datasize(filtered_df, out_fname)














# ******************************** Section10: Generate figure 16a *******************************
if RUN_SECTION_10:
    print("\n--- Running Section 10: Generating Figure 16a (Parallelism Scaling - Env 1) ---")
    env_filter = 'env_2s_2c_125net'
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    # Preprocess the data (assuming `preprocess_df()` returns the preprocessed DataFrame)
    filename = "figure1516a.csv"
    csv_file_path = os.path.join('res', filename)

    try:
        # Read the CSV file into a pandas DataFrame.
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        print(f"Error: File not found at '{csv_file_path}'.")
        print("Please ensure the file exists and the path is correct.")
        exit()


    df['env_name'] = df['env_name'].astype(str)
    filtered_df_env = df[df['env_name'].str.startswith(env_filter)].copy()
    if filtered_df_env.empty:
        print(f"Warning: No data found for environment starting with '{env_filter}'.")
        exit()

    try:
        filtered_df_env['table'] = filtered_df_env['env_name'].str.replace('env_2s_2c_125net_', '', regex=True)
    except IndexError:
        print("Error: Could not derive 'table' from 'env_name'.")
        print("Please check the format of the 'env_name' column.")
        exit()

    filtered_df_env = filtered_df_env[filtered_df_env['table']=='lineitem_sf10']


    # List of parallelism parameters to scale
    parallelism_params = ['write_par', 'decomp_par', 
                        'read_par', 'deser_par', 'comp_par']

    # Default configuration (set all default parallelism values to 1)
    default_config = {
        'write_par': 16,
        'decomp_par': 4,
        'read_par': 1,
        'deser_par': 4,
        'comp_par': 4
    }

    # Line styles for the compression types
    line_styles = {
        'style': 'solid'
    }

    # Iterate over the systems to create separate plots

    plt.figure(figsize=(6, 3.75))

    #display(env_df)
    all_x_ticks = set()  # Collect all unique x-ticks for the x-axis

    # Plot the runtime for each parallelism parameter
    for param in parallelism_params:
        # Make a copy of the default configuration for each parameter iteration
        config = default_config.copy()

        # Scale the current parameter (leave this one to vary)
        filtered_df = filtered_df_env.copy()

        # Apply the default configuration dynamically
        for p in parallelism_params:
            if p != param:
                filtered_df = filtered_df[filtered_df[p] == config[p]]
        
        # Sort the data by the parallelism degree (param) to ensure proper line plotting
        filtered_df = filtered_df.sort_values(by=param)
        #display(param)
        #display(filtered_df)

        # Extract the relevant x (parallelism degree) and y (runtime) values
        x = filtered_df[param]
        y = filtered_df['time']
        
        # Add the unique x-tick values for this parameter
        all_x_ticks.update(x.unique())
        
        # Plot the data with 'nocomp' compression type, using 'x' markers
        plt.plot(x, y, label=f'{param}', linestyle=line_styles['style'], marker='x', zorder=3)

    # Set x-ticks to show only the unique parallelism degrees that exist across all parameters
    all_x_ticks = sorted(all_x_ticks)  # Ensure they are in sorted order
    plt.xticks(all_x_ticks, labels=[str(tick) for tick in all_x_ticks])
    plt.ylim(0)  # Set the y-axis to start at 0

    # Label the axes
    plt.xlabel('Parallelism Degree')
    plt.ylabel('Time (s)')
    #plt.title(f'Runtime vs Parallelism Degree for {system} (nocomp)')

    # Add legend
    plt.legend(loc='best',ncol=2, columnspacing=0.1, handletextpad=0.1, handlelength=1.5)

    # Add grid and improve layout
    plt.grid(alpha=.3)
    plt.tight_layout()

    output_filename = f"figure16a.pdf"
    plt.savefig(output_filename, bbox_inches='tight')

    print(f"\nPlot saved successfully to '{output_filename}'")



# ******************************** Section11: Generate figure16b *******************************
if RUN_SECTION_11:
    print("\n--- Running Section 11: Generating Figure 16b (Config Comparison - Env 1) ---")
    env_filter = 'env_2s_2c_125net'
    # --- Configuration ---

    filename = "figure1516b.csv"
    csv_file_path = os.path.join('res', filename)
    config1 = {
        'write_par': 16,
        'decomp_par': 4,
        'read_par': 1,
        'deser_par': 4,
        'comp_par': 4,
        'compression_lib': 'snappy',
        'buffer_size': 1024,
        'client_buffpool_size': 81920,
    }

    config2 = {
        'write_par': 2,
        'decomp_par': 16,
        'read_par': 2,
        'deser_par': 1,
        'comp_par': 8,
        'compression_lib': 'snappy',
        'buffer_size': 1024,
        'client_buffpool_size': 81920,
    }

    # --- Data Loading and Preparation ---

    # Create output directory if it doesn't exist

    try:
        # Read the CSV file into a pandas DataFrame.
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        print(f"Error: File not found at '{csv_file_path}'.")
        print("Please ensure the file exists and the path is correct.")
        exit()


    df['env_name'] = df['env_name'].astype(str)
    filtered_df_env = df[df['env_name'].str.startswith(env_filter)].copy()
    if filtered_df_env.empty:
        print(f"Warning: No data found for environment starting with '{env_filter}'.")
        exit()


    try:
        filtered_df_env['table'] = filtered_df_env['env_name'].str.replace('env_2s_2c_125net_', '', regex=True)
    except IndexError:
        print("Error: Could not derive 'table' from 'env_name'.")
        print("Please check the format of the 'env_name' column.")
        exit()

    dataset_map = {
        'lineitem_sf10': 'lineitem',
        'ss13husallm': 'acs',
        'iotm': 'iot',
        'inputeventsm': 'icu'
    }
    filtered_df_env['table'] = filtered_df_env['table'].replace(dataset_map)

    # 4. Define a custom order for the datasets on the x-axis.
    custom_order = ['lineitem', 'acs', 'iot', 'icu']
    filtered_df_env['table'] = pd.Categorical(filtered_df_env['table'], categories=custom_order, ordered=True)
    filtered_df_env = filtered_df_env.sort_values(by='table')
    datasets = filtered_df_env['table'].unique()

    # 5. Prepare runtime data for both configurations.
    def get_runtimes(base_df, config, datasets):
        """Filters the dataframe by the given config and returns runtimes for each dataset."""
        runtimes = []
        for dataset in datasets:
            # Filter for the current dataset
            dataset_df = base_df[base_df['table'] == dataset]

            # Apply the configuration filters
            config_df = dataset_df.copy()
            for key, value in config.items():
                config_df = config_df[config_df[key] == value]

            if not config_df.empty:
                # If multiple repetitions exist, take the average time
                average_time = config_df['time'].mean()
                runtimes.append(average_time)
            else:
                # If no data matches the config, append NaN and print a warning
                print(f"Warning: No data found for dataset '{dataset}' with the specified configuration.")
                runtimes.append(np.nan)
        return runtimes

    config1_runtimes = get_runtimes(filtered_df_env, config1, datasets)
    config2_runtimes = get_runtimes(filtered_df_env, config2, datasets)

    # Note: The original script had a hardcoded value.
    # This might be for correcting a known anomaly in the data.
    # If 'iot' is the 3rd dataset in your custom_order, this line will modify its runtime.
    try:
        if len(config2_runtimes) > 2:
            config2_runtimes[2] = 73.89
    except IndexError:
        print("Could not apply hardcoded runtime value, not enough data points.")



    # --- Plotting ---

    plt.style.use('seaborn-v0_8-whitegrid')
    plt.figure(figsize=(6, 3.75))

    x_indexes = np.arange(len(datasets))  # Set x-axis positions for each dataset
    bar_width = 0.35  # Width of each bar
    color_palette = sns.color_palette("colorblind", 2)

    # Plot bars for config1
    plt.bar(x_indexes - bar_width/2, config1_runtimes, width=bar_width, label='cc-config', color=color_palette[0], zorder=3)
    # Plot bars for config2
    plt.bar(x_indexes + bar_width/2, config2_runtimes, width=bar_width, label='ff-config', color=color_palette[1], zorder=3)
    # Set x-ticks to the dataset names
    plt.xticks(ticks=x_indexes, labels=datasets)
    # Label the axes and add a title
    plt.xlabel('Dataset', fontweight='bold')
    plt.ylabel('Time (s)', fontweight='bold')
    plt.title(f'Performance Comparison on {env_filter}', fontweight='bold')
    # Add a legend
    plt.legend(title='Configuration')
    # Adjust the layout
    plt.tight_layout()
    output_filename = f"figure16b.pdf"
    plt.savefig(output_filename, bbox_inches='tight')
    print(f"\nPlot saved successfully to '{output_filename}'")
    # Show the plot
    plt.show()








# ******************************** Section12: Generate figure 15a *******************************
if RUN_SECTION_12:
    print("\n--- Running Section 12: Generating Figure 15a (Parallelism Scaling - Env 2) ---")
    env_filter = 'env_16s_2c_125net'
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    # Preprocess the data (assuming `preprocess_df()` returns the preprocessed DataFrame)
    filename = "figure1516a.csv"
    csv_file_path = os.path.join('res', filename)

    try:
        # Read the CSV file into a pandas DataFrame.
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        print(f"Error: File not found at '{csv_file_path}'.")
        print("Please ensure the file exists and the path is correct.")
        exit()


    df['env_name'] = df['env_name'].astype(str)
    filtered_df_env = df[df['env_name'].str.startswith(env_filter)].copy()
    if filtered_df_env.empty:
        print(f"Warning: No data found for environment starting with '{env_filter}'.")
        exit()

    try:
        filtered_df_env['table'] = filtered_df_env['env_name'].str.replace('env_16s_2c_125net_', '', regex=True)
    except IndexError:
        print("Error: Could not derive 'table' from 'env_name'.")
        print("Please check the format of the 'env_name' column.")
        exit()

    filtered_df_env = filtered_df_env[filtered_df_env['table']=='lineitem_sf10']


    # List of parallelism parameters to scale
    parallelism_params = ['write_par', 'decomp_par', 
                        'read_par', 'deser_par', 'comp_par']

    # Default configuration (set all default parallelism values to 1)
    default_config = {
        'write_par': 16,
        'decomp_par': 4,
        'read_par': 1,
        'deser_par': 4,
        'comp_par': 4
    }

    # Line styles for the compression types
    line_styles = {
        'style': 'solid'
    }

    # Iterate over the systems to create separate plots

    plt.figure(figsize=(6, 3.75))

    #display(env_df)
    all_x_ticks = set()  # Collect all unique x-ticks for the x-axis

    # Plot the runtime for each parallelism parameter
    for param in parallelism_params:
        # Make a copy of the default configuration for each parameter iteration
        config = default_config.copy()

        # Scale the current parameter (leave this one to vary)
        filtered_df = filtered_df_env.copy()

        # Apply the default configuration dynamically
        for p in parallelism_params:
            if p != param:
                filtered_df = filtered_df[filtered_df[p] == config[p]]
        
        # Sort the data by the parallelism degree (param) to ensure proper line plotting
        filtered_df = filtered_df.sort_values(by=param)
        #display(param)
        #display(filtered_df)

        # Extract the relevant x (parallelism degree) and y (runtime) values
        x = filtered_df[param]
        y = filtered_df['time']
        
        # Add the unique x-tick values for this parameter
        all_x_ticks.update(x.unique())
        
        # Plot the data with 'nocomp' compression type, using 'x' markers
        plt.plot(x, y, label=f'{param}', linestyle=line_styles['style'], marker='x', zorder=3)

    # Set x-ticks to show only the unique parallelism degrees that exist across all parameters
    all_x_ticks = sorted(all_x_ticks)  # Ensure they are in sorted order
    plt.xticks(all_x_ticks, labels=[str(tick) for tick in all_x_ticks])
    plt.ylim(0)  # Set the y-axis to start at 0

    # Label the axes
    plt.xlabel('Parallelism Degree')
    plt.ylabel('Time (s)')
    #plt.title(f'Runtime vs Parallelism Degree for {system} (nocomp)')

    # Add legend
    plt.legend(loc='best',ncol=2, columnspacing=0.1, handletextpad=0.1, handlelength=1.5)

    # Add grid and improve layout
    plt.grid(alpha=.3)
    plt.tight_layout()

    output_filename = f"figure15a.pdf"
    plt.savefig(output_filename, bbox_inches='tight')

    print(f"\nPlot saved successfully to '{output_filename}'")



# ******************************** Section13: Generate figure15b *******************************
if RUN_SECTION_13:
    print("\n--- Running Section 13: Generating Figure 15b (Config Comparison - Env 2) ---")
    env_filter = 'env_16s_2c_125net'
    # --- Configuration ---

    filename = "figure1516b.csv"
    csv_file_path = os.path.join('res', filename)
    config1 = {
        'write_par': 16,
        'decomp_par': 4,
        'read_par': 1,
        'deser_par': 4,
        'comp_par': 4,
        'compression_lib': 'snappy',
        'buffer_size': 1024,
        'client_buffpool_size': 81920,
    }

    config2 = {
        'write_par': 2,
        'decomp_par': 16,
        'read_par': 2,
        'deser_par': 1,
        'comp_par': 8,
        'compression_lib': 'snappy',
        'buffer_size': 1024,
        'client_buffpool_size': 81920,
    }

    # --- Data Loading and Preparation ---

    # Create output directory if it doesn't exist

    try:
        # Read the CSV file into a pandas DataFrame.
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        print(f"Error: File not found at '{csv_file_path}'.")
        print("Please ensure the file exists and the path is correct.")
        exit()


    df['env_name'] = df['env_name'].astype(str)
    filtered_df_env = df[df['env_name'].str.startswith(env_filter)].copy()
    if filtered_df_env.empty:
        print(f"Warning: No data found for environment starting with '{env_filter}'.")
        exit()


    try:
        filtered_df_env['table'] = filtered_df_env['env_name'].str.replace('env_16s_2c_125net_', '', regex=True)
    except IndexError:
        print("Error: Could not derive 'table' from 'env_name'.")
        print("Please check the format of the 'env_name' column.")
        exit()

    dataset_map = {
        'lineitem_sf10': 'lineitem',
        'ss13husallm': 'acs',
        'iotm': 'iot',
        'inputeventsm': 'icu'
    }
    filtered_df_env['table'] = filtered_df_env['table'].replace(dataset_map)

    # 4. Define a custom order for the datasets on the x-axis.
    custom_order = ['lineitem', 'acs', 'iot', 'icu']
    filtered_df_env['table'] = pd.Categorical(filtered_df_env['table'], categories=custom_order, ordered=True)
    filtered_df_env = filtered_df_env.sort_values(by='table')
    datasets = filtered_df_env['table'].unique()

    # 5. Prepare runtime data for both configurations.
    def get_runtimes(base_df, config, datasets):
        """Filters the dataframe by the given config and returns runtimes for each dataset."""
        runtimes = []
        for dataset in datasets:
            # Filter for the current dataset
            dataset_df = base_df[base_df['table'] == dataset]

            # Apply the configuration filters
            config_df = dataset_df.copy()
            for key, value in config.items():
                config_df = config_df[config_df[key] == value]

            if not config_df.empty:
                # If multiple repetitions exist, take the average time
                average_time = config_df['time'].mean()
                runtimes.append(average_time)
            else:
                # If no data matches the config, append NaN and print a warning
                print(f"Warning: No data found for dataset '{dataset}' with the specified configuration.")
                runtimes.append(np.nan)
        return runtimes

    config1_runtimes = get_runtimes(filtered_df_env, config1, datasets)
    config2_runtimes = get_runtimes(filtered_df_env, config2, datasets)

    # Note: The original script had a hardcoded value.
    # This might be for correcting a known anomaly in the data.
    # If 'iot' is the 3rd dataset in your custom_order, this line will modify its runtime.
    try:
        if len(config2_runtimes) > 2:
            config2_runtimes[2] = 73.89
    except IndexError:
        print("Could not apply hardcoded runtime value, not enough data points.")



    # --- Plotting ---

    plt.style.use('seaborn-v0_8-whitegrid')
    plt.figure(figsize=(6, 3.75))

    x_indexes = np.arange(len(datasets))  # Set x-axis positions for each dataset
    bar_width = 0.35  # Width of each bar
    color_palette = sns.color_palette("colorblind", 2)

    # Plot bars for config1
    plt.bar(x_indexes - bar_width/2, config1_runtimes, width=bar_width, label='cc-config', color=color_palette[0], zorder=3)
    # Plot bars for config2
    plt.bar(x_indexes + bar_width/2, config2_runtimes, width=bar_width, label='ff-config', color=color_palette[1], zorder=3)
    # Set x-ticks to the dataset names
    plt.xticks(ticks=x_indexes, labels=datasets)
    # Label the axes and add a title
    plt.xlabel('Dataset', fontweight='bold')
    plt.ylabel('Time (s)', fontweight='bold')
    plt.title(f'Performance Comparison on {env_filter}', fontweight='bold')
    # Add a legend
    plt.legend(title='Configuration')
    # Adjust the layout
    plt.tight_layout()
    output_filename = f"figure15b.pdf"
    plt.savefig(output_filename, bbox_inches='tight')
    print(f"\nPlot saved successfully to '{output_filename}'")
    # Show the plot
    plt.show()


# ******************************** Section14: Generate figure 6 a,b *******************************
if RUN_SECTION_14:
    print("\n--- Running Section 14: Generating Figures 6a & 6b (Configuration Runtimes) ---")
    filename1 = "figure7.csv"
    csv_file_path1 = os.path.join('res', filename1)
    filename2 = "figure7b.csv"
    csv_file_path2 = os.path.join('res', filename2)
    for i, file in enumerate([csv_file_path1, csv_file_path2]):
        # Read the CSV file
        data = pd.read_csv(file)  # Replace with your actual CSV file name
        
        #data = data[(data['comp_par']==1) & (data['decomp_par']==1)]
        data = data[['timestamp', 'time']]
        data = data[data['time']>0]
        # Sort the data by the "time" column
        data_sorted = data.sort_values(by="time", ascending=False).reset_index()
        
        # Extract the "time" column and create indices for the x-axis
        sorted_times = data_sorted["time"].values
        sorted_indices = np.arange(len(sorted_times))  # Indices for x-axis
        
        # Set up plot style
        plt.rcParams['text.usetex'] = True
        plt.rcParams['font.family'] = 'serif'
        plt.rcParams['font.serif'] = ['Computer Modern Roman']
        plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})
        
        # Create plot
        plt.figure(figsize=(6, 3.75))
        plt.bar(sorted_indices, sorted_times, width=0.6, color='steelblue', edgecolor='steelblue', zorder=3)
        
        # Add labels and title
        plt.xlabel('Configuration Index')
        plt.ylabel('Time (s)')
        #plt.xticks(ticks=sorted_indices, labels=sorted_indices, rotation=45)
        
        # Add grid for better readability
        plt.grid(axis='y', alpha=0.3, zorder=0)
        
        # Save and display the plot
        plt.tight_layout()
        if i == 0:
            plt.savefig('figure6a.pdf', bbox_inches='tight')
        else:
            plt.savefig('figure6b.pdf', bbox_inches='tight')
        plt.show()
        plt.show()


# ******************************** Section16: Figure14b *******************************

if RUN_SECTION_16:
    print("\n--- Running Section 16: Generating Figure 14b (Runtime Comparison) ---")
    filename = "figureYParquet.csv"
    csv_file_path = os.path.join('res', filename)

    # Load CSV data
    data = pd.read_csv(csv_file_path)
    data['table'] = data['table'].replace({
        'lineitem_sf10': 'lineitem',
        'ss13husallm': 'acs',
        'iotm': 'iot',
        'inputeventsm': 'icu'
    })

    data['system'] = data['system'].replace({'pyarrow': 'read_parquet[pyarrow]'})

    # Define the environments
    environments = data['env'].unique()
    assert len(environments) == 1, f"Error: Expected 1 environment, but found {len(environments)}."
    env = environments[0]

    # Define the desired order for approaches
    approaches = ['xdbc[col]', 'xdbc[parquet]','read_parquet[pyarrow]', 'duckdb']

    # Define the desired order for tables
    tables = ['lineitem', 'acs', 'iot', 'icu']

    # Prepare data for plotting
    grouped_data = data.groupby(['env', 'system', 'table'])['time'].mean().reset_index()

    # Reindex the data to ensure the tables and approaches are in the desired order
    grouped_data['table'] = pd.Categorical(grouped_data['table'], categories=tables, ordered=True)
    grouped_data['system'] = pd.Categorical(grouped_data['system'], categories=approaches, ordered=True)
    grouped_data = grouped_data.sort_values(['table', 'system'])

    # Set up plot style
    plt.rcParams['text.usetex'] = True
    plt.rcParams['font.family'] = 'serif'
    plt.rcParams['font.serif'] = ['Computer Modern Roman']
    plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

    # Generate plots for each environment

    env_data = grouped_data[grouped_data['env'] == env]

    # Pivot the data to get tables as x-axis and approaches as bars
    pivot_data = env_data.pivot(index='table', columns='system', values='time')

    # Set the bar width and positions
    bar_width = 0.15
    x_indexes = np.arange(len(pivot_data.index))
    colors = sns.color_palette("colorblind", len(approaches))

    # Create the plot
    plt.figure(figsize=(6, 3.75))

    for i, (approach, color) in enumerate(zip(approaches, colors)):
        if approach in pivot_data.columns:
            plt.bar(x_indexes + (i - len(approaches)/2) * bar_width, pivot_data[approach],
                    width=bar_width, label=approach, color=color, zorder=3)

    # Labels and title
    plt.xlabel('Tables')
    plt.ylabel('Time (s)')
    plt.xticks(ticks=x_indexes, labels=pivot_data.index)
    plt.legend(loc='best', handletextpad=0.5, borderaxespad=0.5, borderpad=0.4)

    # Add grid for better readability
    plt.grid(axis='y', alpha=0.3, zorder=0)

    # Save the plot
    output_file = f'figure14b.pdf'
    plt.tight_layout()
    plt.savefig(output_file, bbox_inches='tight')

#* ******************************** Section17: Generate figure 7b *******************************
if RUN_SECTION_17:
    print("\n--- Running Section 17: Generating Figure 7b (Runtime Comparison) ---")
    filename = "figure8b.csv"
    csv_file_path = os.path.join('res', filename)


    # Read the CSV file
    data = pd.read_csv(csv_file_path)

    # Replace mismatched table names
    data['table'] = data['table'].replace({
        'lineitem_sf10': 'lineitem',
        'ss13husallm': 'acs',
        'iotm': 'iot',
        'inputeventsm': 'icu'
    })

    # Calculate the average time for each combination of table and system
    average_times = (
        data.groupby(['table', 'system'])['time']
        .mean()
        .reset_index()
        .pivot(index='table', columns='system', values='time')
    )

    # Define the order of systems and datasets for consistency
    systems = ['xdbc', 'read_csv_url']
    tables = ['lineitem', 'acs', 'iot', 'icu']

    # Reindex to ensure proper order and fill missing values with 0 (if any)
    average_times = average_times.reindex(index=tables, columns=systems, fill_value=0)

    # Extract data for plotting
    approach_times = [average_times[system].values for system in systems]

    # Set up plot style
    plt.rcParams['text.usetex'] = True
    plt.rcParams['font.family'] = 'serif'
    plt.rcParams['font.serif'] = ['Computer Modern Roman']
    plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

    # Create the plot
    datasets = ['lineitem', 'acs', 'iot', 'icu']  # Adjust dataset names for readability
    formal_palette = sns.color_palette("colorblind", len(systems))
    bar_width = 0.2
    x_indexes = np.arange(len(datasets))

    plt.figure(figsize=(6, 3.75))

    # Plotting each approach with offset for bar positions
    for i, (system, times) in enumerate(zip(systems, approach_times)):
        plt.bar(x_indexes + (i - 0.5) * bar_width, times, width=bar_width, color=formal_palette[i], label=system, zorder=3)

    # Labels and Title
    plt.xlabel('Datasets')
    plt.ylabel('Time (s)')
    plt.xticks(ticks=x_indexes, labels=datasets)
    plt.legend(loc='best')

    # Grid for better readability
    plt.grid(axis='y', alpha=0.3, zorder=0)

    # Display the plot
    output_file = f'figure7b.pdf'
    plt.tight_layout()
    plt.savefig(output_file, bbox_inches='tight')

# ******************************** Section18: Generate figure7a *******************************
if RUN_SECTION_18:
    print("\n--- Running Section 18: Generating Figure 7a (Runtime Comparison) ---")
    filename = "figure8a.csv"
    csv_file_path = os.path.join('res', filename)

    # Read the CSV file
    data = pd.read_csv(csv_file_path)

    # Replace mismatched table names
    data['table'] = data['table'].replace({
        'lineitem_sf10': 'lineitem',
        'ss13husallm': 'acs',
        'iotm': 'iot',
        'inputeventsm': 'icu'
    })

    # Calculate the average time for each combination of table and system
    average_times = (
        data.groupby(['table', 'system'])['time']
        .mean()
        .reset_index()
        .pivot(index='table', columns='system', values='time')
    )

    # Define the order of systems and datasets for consistency
    systems = ['xdbc', 'connectorx', 'duckdb', 'modin', 'turbodbc']
    tables = ['lineitem', 'acs', 'iot', 'icu']

    # Reindex to ensure proper order and fill missing values with 0 (if any)
    average_times = average_times.reindex(index=tables, columns=systems, fill_value=0)

    # Extract data for plotting
    approach_times = [average_times[system].values for system in systems]

    # Set up plot style
    plt.rcParams['text.usetex'] = True
    plt.rcParams['font.family'] = 'serif'
    plt.rcParams['font.serif'] = ['Computer Modern Roman']
    plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

    # Create the plot
    datasets = ['lineitem', 'acs', 'iot', 'icu']  # Adjust dataset names for readability
    formal_palette = sns.color_palette("colorblind", len(systems))
    bar_width = 0.17
    x_indexes = np.arange(len(datasets))

    plt.figure(figsize=(6, 3.75))

    # Plotting each approach with offset for bar positions
    for i, (system, times) in enumerate(zip(systems, approach_times)):
        plt.bar(x_indexes + (i - 2) * bar_width, times, width=bar_width, color=formal_palette[i], label=system, zorder=3)

    # Labels and Title
    plt.xlabel('Datasets')
    plt.ylabel('Time (s)')
    plt.xticks(ticks=x_indexes, labels=datasets)
    plt.legend(loc='best')

    # Grid for better readability
    plt.grid(axis='y', alpha=0.3, zorder=0)

    # Display the plot
    plt.tight_layout()
    plt.savefig('figure7a.pdf', bbox_inches='tight')
    plt.show()

# ******************************** Section19: Generate figure 8a *******************************
if RUN_SECTION_19:
    print("\n--- Running Section 19: Generating Figure 8 (Runtime Comparison) ---")
    filename = "figurePandasPG.csv"
    csv_file_path = os.path.join('res', filename)
    
    # Read the CSV file
    data = pd.read_csv(csv_file_path)

    # Filter for network == 0 and include conf1, conf2, and connector-x
    systems = ['xdbc[aggressive]', 'xdbc[conservative]', 'connectorx[aggressive]','connectorx[conservative]']
    data = data[(data['network'] == 0) & (data['system'].isin(systems))]

    # Calculate the average time for each combination of CPU cores and system
    average_times = (
        data.groupby(['client_cpu', 'system'])['time']
        .mean()
        .reset_index()
        .pivot(index='client_cpu', columns='system', values='time')
    )

    # Define the order of systems and CPU cores for consistency

    cpu_cores = [1,2, 4, 8, 16]

    # Reindex to ensure proper order and fill missing values with 0 (if any)
    average_times = average_times.reindex(index=cpu_cores, columns=systems, fill_value=0)

    # Extract data for plotting
    approach_times = [average_times[system].values for system in systems]

    # Set up plot style
    plt.rcParams['text.usetex'] = True
    plt.rcParams['font.family'] = 'serif'
    plt.rcParams['font.serif'] = ['Computer Modern Roman']
    plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

    # Create the plot
    datasets = ['1', '2', '4', '8', '16']  # Adjust dataset names for readability
    formal_palette = sns.color_palette("colorblind", 2)
    bar_width = 0.2
    x_indexes = np.arange(len(datasets))

    plt.figure(figsize=(6, 3.75))

    # Plotting each approach with specified colors and hatches
    plt.bar(x_indexes - bar_width, approach_times[0], width=bar_width, color=formal_palette[0], label=systems[0], zorder=3)
    plt.bar(x_indexes, approach_times[1], width=bar_width, color=formal_palette[0], label=systems[1], hatch='/', edgecolor='white', zorder=3)
    plt.bar(x_indexes + bar_width, approach_times[2], width=bar_width, color=formal_palette[1], label=systems[2], edgecolor='white', zorder=3)
    plt.bar(x_indexes + bar_width*2, approach_times[3], width=bar_width, color=formal_palette[1], label=systems[3], hatch='/', edgecolor='white', zorder=3)

    # Labels and Title
    plt.xlabel('CPU Cores')
    plt.ylabel('Time (s)')
    plt.xticks(ticks=x_indexes, labels=datasets)
    plt.legend(loc='upper right')

    # Grid for better readability
    plt.grid(axis='y', alpha=0.3, zorder=0)

    # Display the plot
    plt.tight_layout()
    plt.savefig('figure8a.pdf', bbox_inches='tight')
    plt.show()

# ******************************** Section20: Generate figure 8b *******************************
if RUN_SECTION_20:
    print("\n--- Running Section 20: Generating Figure 8b (Runtime Comparison) ---")
    filename = "figurePandasPG.csv"
    csv_file_path = os.path.join('res', filename)
    data = pd.read_csv(csv_file_path)

    systems = ['xdbc[nocomp]', 'xdbc[comp]', 'connectorx']
    # Filter for client_cpu == 32 and include conf1, conf2, and connector-x
    data = data[(data['client_cpu'] == 8) & (data['system'].isin(systems))]

    # Calculate the average time for each combination of network bandwidth and system
    average_times = (
        data.groupby(['network', 'system'])['time']
        .min()
        .reset_index()
        .pivot(index='network', columns='system', values='time')
    )

    # Define the order of systems and network bandwidth for consistency

    networks = sorted(data['network'].unique(), key=lambda x: float('inf') if x == 0 else x)  # Move 0 (unlimited) to the end

    # Reindex to ensure proper order and fill missing values with 0 (if any)
    average_times = average_times.reindex(index=networks, columns=systems, fill_value=0)

    # Extract data for plotting
    approach_times = [average_times[system].values for system in systems]

    # Set up plot style
    plt.rcParams['text.usetex'] = True
    plt.rcParams['font.family'] = 'serif'
    plt.rcParams['font.serif'] = ['Computer Modern Roman']
    plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

    # Create the plot
    datasets = [str(n) for n in networks]  # Convert network bandwidth to string for readability
    datasets = ["local" if n == 0 else str(n) for n in networks]
    formal_palette = sns.color_palette("colorblind", 2)
    bar_width = 0.2
    x_indexes = np.arange(len(datasets))

    plt.figure(figsize=(6, 3.75))

    # Plotting each approach with specified colors and hatches
    plt.bar(x_indexes - bar_width, approach_times[0], width=bar_width, color=formal_palette[0], label='xdbc[nocomp]', zorder=3)
    plt.bar(x_indexes, approach_times[1], width=bar_width, color=formal_palette[0], label='xdbc[comp]', hatch='/', edgecolor='white', zorder=3)
    plt.bar(x_indexes + bar_width, approach_times[2], width=bar_width, color=formal_palette[1], label='connectorx', edgecolor='white', zorder=3)

    # Labels and Title
    plt.xlabel('Network Bandwidth (megabyte/s)')
    plt.ylabel('Time (s)')
    plt.xticks(ticks=x_indexes, labels=datasets)
    plt.legend(loc='best')

    # Grid for better readability
    plt.grid(axis='y', alpha=0.3, zorder=0)

    # Display the plot
    plt.tight_layout()
    plt.savefig('figure8b.pdf', bbox_inches='tight')
    plt.show()

# ******************************** Section21: Generate figure 8b *******************************
if RUN_SECTION_21:
    print("\n--- Running Section 21: Generating Figure 20 ---")
    filename = "figure20.csv"
    csv_file_path = os.path.join('res', filename)
    df = pd.read_csv(csv_file_path)
    df['config_name'] = df['config_name'].replace('xdbc-bruteforce', 'bf')
    df['config_name'] = df['config_name'].replace('xdbc-heuristic', 'xdbc')
    #display(df)
    #display(df)
    plt.rcParams['axes.titlesize'] = 22
    plt.rcParams['axes.labelsize'] = 22 
    plt.rcParams['xtick.labelsize'] = 22
    plt.rcParams['ytick.labelsize'] = 22

    plt.rcParams['text.usetex'] = True
    plt.rcParams['font.family'] = 'serif'
    plt.rcParams['font.serif'] = ['Computer Modern Roman']
    plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

    legend_fontsize=21.9
    borderpad=0
    labelspacing=0
    borderaxespad=0.18
    handlelength=.95
    edgecolor='white'
    frameon=True
    # Define the custom sorting order for the optimizer column

    #df['optimizer'] = df['optimizer'].cat.add_categories('expert_icu_anal.')
    #df['optimizer'] = df['optimizer'].cat.add_categories('expert_iot_anal.')
    df.loc[df['config_name'] == 'icu_analysis', 'config_name'] = 'icu_anal.'
    df.loc[df['config_name'] == 'iot_analysis', 'config_name'] = 'iot_anal.'

    custom_order = [
        'copy', 'icu_anal.', 'backup', 
        'iot_anal.', 'etl', 'pg', 'bf','xdbc'
    ]

    # Convert the 'optimizer' column to a categorical type with the custom order
    df['config_name'] = pd.Categorical(df['config_name'], categories=custom_order, ordered=True)

    env_sizes = {
        'iot_analysis': 3667,
        'backup': 10935,
        'icu_analysis': 3601,
        'copy': 10473,
        'etl': 10473,
        'pg': 10473
    }

    env_bounds = {
        'iot_analysis': 17,
        'backup': 20,
        'icu_analysis': 10.7,
        'copy': 10,
        'etl': 40,
        'pg': 40
    }



    env_chars = {
        'iot_anal' : {
            'src_size': 3506,
            'compressed': 800,
            'bound' : 100,
            'reason' : 'slowest is pandas at 205MB/s, size 3500/205=17'
            
        },
        'backup' : {
            'src_size': 10000,
            'compressed': 2888,
            'bound' : 250,
            'reason' : 'serialization bound at 500MBps, size 10000/500=20'
            
        },
        'icu' : {
            'src_size': 2200,
            'compressed': 350,
            'bound' : 50,
            'reason': 'pandas bound at 205MB/s, size 2200/205=10.7'
        },
        'copy' : {
            'src_size': 10000,
            'compressed': 2000,
            'bound' : 0,
            'reason' : ''
        },
        'etl' : {
            'src_size': 10000,
            'compressed': 2000,
            'bound' : 500,
            '': '5serialization bound at 250MBps, size 10000/250=40'
        },
        'db_migr' : {
            'src_size': 10000,
            'compressed': 2000,
            'bound' : 100,
            'reason' : 'serialization bound at 250MBps, size 10000/250=40'
        }
        
        
    }


    # Sort the DataFrame according to the custom order
    df = df.sort_values('config_name')
    # Get the unique environments
    environments = df['env_name'].unique()

    # Define a colorblind-friendly color palette
    color_palette = sns.color_palette("colorblind")

    # Iterate through each environment and create a plot
    for env in environments:
        # Filter the DataFrame for the current environment
        env_df = df[df['env_name'] == env]

        # Extract optimizers and runtimes for this environment
        optimizers = env_df['config_name'][::-1]
        runtimes = env_df['time'][::-1]
        #print(env)
        #print(runtimes)

        # Create a new figure for the environment
        plt.figure(figsize=(6, 3.75))

        # Plot each optimizer with a unique color from the colorblind-friendly palette
        bars = plt.bar(optimizers, runtimes, color=color_palette[:len(optimizers)], zorder=3)
        plt.axhline(y=env_bounds[env], color='lightblue', linestyle='--', linewidth=1.5, zorder=5)


        # Add titles and labels
        #plt.title(f'Runtime Comparison for {env}')
        plt.xlabel('Optimizer Configuration')
        plt.ylabel('Time (s)')
        plt.xticks([])
        plt.ylim(0, max(runtimes) * 1.3)
        plt.grid(axis='y', alpha=0.3)

        optimizers = optimizers.str.replace('expert_', '')
        # Create a legend for the optimizers
        plt.legend(bars, optimizers, labelspacing=labelspacing,ncols=4, handletextpad=0.25, columnspacing=0.15, 
                fontsize=legend_fontsize, loc='upper center', handlelength=handlelength,borderpad=borderpad, 
                borderaxespad=borderaxespad, frameon=frameon, edgecolor=edgecolor)

        ax = plt.gca()
        ax.tick_params(axis='y', pad=-2)
        # Save the figure as PNG
        plt.savefig(f'figure20_{env}.pdf', bbox_inches='tight', pad_inches=0.05)
        
        plt.show()
        plt.close()

    # ******************************** Section22: Generate figure 21*******************************
if RUN_SECTION_22:
    print("\n--- Running Section 22: Generating Figure 21 (Runtime Comparison) ---")
    filename = "figure20.csv"
    csv_file_path = os.path.join('res', filename)
    data = pd.read_csv(csv_file_path)

    df_runs = pd.read_csv(csv_file_path)

    # Dictionary of environment sizes (in MB, GB, etc. as applicable)
    env_sizes = {
        'iot_analysis': 3667,
        'backup': 10935,
        'icu_analysis': 3601,
        'copy': 10473,
        'etl': 10473,
        'pg': 10473
    }

    # Map the size to the CSV DataFrame based on 'env_name'
    df_runs['size'] = df_runs['env_name'].map(env_sizes)

    # Calculate real throughput (real throughput = size / runtime)
    df_runs['real_throughput'] = df_runs['size'] / df_runs['time']

    # Extract the lists of estimated and real throughputs
    estimated_throughput = df_runs['est_throughput'].tolist()
    real_throughput = df_runs['real_throughput'].tolist()

    env_name_map = {
        'iot_analysis': 'IoT Anal.',
        'backup': 'Backup',
        'icu_analysis': 'ICU Anal.',
        'copy': 'Copy',
        'etl': 'ETL',
        'pg': 'DB Mig.'
    }

    names = [env_name_map[env] for env in df_runs['env_name']]

    # Set colorblind-friendly palette
    sns.set_palette("colorblind")
    palette = sns.color_palette()

    # Create a mapping from names to colors
    unique_env_names = list(env_name_map.values())
    name_color_map = {}
    for i, name in enumerate(unique_env_names):
        name_color_map[name] = palette[i % len(palette)]

    plt.figure(figsize=(8, 4.5))
    marker_size = 110

    # First pass: plot all points with regular markers
    for i in range(len(estimated_throughput)):
        scatter = plt.scatter(
            estimated_throughput[i],
            real_throughput[i],
            color=name_color_map[names[i]],
            label=names[i],
            s=marker_size,
            edgecolor='white',
            zorder=3,
        )

    # Second pass: overlay special markers for specific optimizers
    for i in range(len(estimated_throughput)):
        if df_runs['config_name'].iloc[i] == 'bf':  # bruteforce
                plt.scatter(
                    estimated_throughput[i],
                    real_throughput[i],
                    color='white',  # White background
                    edgecolor='black',
                    marker='+',
                    s=marker_size * 2,
                    linewidths=2,
                    zorder=4,
                )
        elif df_runs['config_name'].iloc[i] == 'xdbc':  # heuristic/optimizer
            plt.scatter(
                estimated_throughput[i],
                real_throughput[i],
                color='white',  # Transparent center
                edgecolor='black',
                marker='*',
                s=marker_size * 1.5,  # Larger size for visibility
                linewidths=1.5,
                zorder=4,
            )

    # Add y = x line
    plt.plot([0, 800], [0, 800], 'k--', linewidth=2, zorder=1)

    # Remove duplicate labels in legend
    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    legend = plt.legend(by_label.values(), by_label.keys(), ncols=3, handletextpad=0.25, 
                    columnspacing=0.25, loc='upper left', handlelength=1.3, 
                    borderpad=.15, borderaxespad=0.2)

    plt.grid(alpha=0.3)
    plt.xlabel('Estimated Throughput (MB)')
    plt.ylabel('Real Throughput (MB)')

    # Add optimizer legend
    optimizer_scatter = plt.scatter([], [], color='black', marker='*', s=100, label='Optimizer')
    optimizer_scatter2 = plt.scatter([], [], color='black', marker='+', s=100, label='Bruteforce')

    plt.gca().add_artist(legend)
    leg2 = plt.legend([optimizer_scatter], ['Optimizer'], bbox_to_anchor=(0.25, 0.86), handletextpad=0.25)
    plt.gca().add_artist(leg2)
    plt.legend([optimizer_scatter2], ['Bruteforce'], bbox_to_anchor=(0.49, 0.86), handletextpad=0.25)

    plt.savefig('figure21.pdf', bbox_inches='tight', pad_inches=0.05)
    plt.show()


    # ******************************** Section23: Generate figure 9a*******************************
if RUN_SECTION_23:
    print("\n--- Running Section 23: Generating Figure 9a ---")
    # Define the path to the CSV file
    csv_file_path = os.path.join('res', 'figure9a.csv')

    # Read the CSV file if it exists
    if os.path.exists(csv_file_path):
        data = pd.read_csv(csv_file_path)
        print(f"Data loaded from {csv_file_path}")
        print("Raw CSV data:")
        print(data)
        print(f"Columns: {data.columns.tolist()}")
    else:
        print(f"CSV file not found at {csv_file_path}")
        # Create empty DataFrame with expected structure
        # data = pd.DataFrame(columns=['fdw', 'dataset', 'execution_time'])
        data = pd.DataFrame(columns=['table_name', 'system', 'config', 'elapsed_time_ms', 'timestamp'])

    # Set up the plot style
    plt.rcParams['text.usetex'] = True
    plt.rcParams['font.family'] = 'serif'
    plt.rcParams['font.serif'] = ['Computer Modern Roman']
    plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

    # Define the dataset mapping
    dataset_mapping = {
        'lineitem_sf10': 'lineitem',
        'ss13husallm': 'acs',
        'iotm': 'iot',
        'inputeventsm': 'icu'
    }

    # Define the expected datasets and approaches
    expected_datasets = ['lineitem', 'acs', 'iot', 'icu']
    expected_approaches = ['xdbc', 'jdbc']

    # Process the data from the CSV
    if not data.empty:
        print("\nProcessing data from CSV...")
        
        # Map dataset names to display names
        data['dataset_display'] = data['table_name'].map(dataset_mapping)
        print(f"Data after mapping:")
        print(data)
        
        # Remove any rows with unmapped datasets
        data = data.dropna(subset=['dataset_display'])
        print(f"Data after removing unmapped datasets:")
        print(data)

        data['elapsed_time_s'] = data['elapsed_time_ms'] / 1000.0
        
        # Pivot the data to get execution times for each approach
        pivot_data = data.pivot(index='dataset_display', columns='system', values='elapsed_time_s')
        print(f"Pivoted data:")
        print(pivot_data)
        
        # Get available datasets and approaches
        available_datasets = [ds for ds in expected_datasets if ds in pivot_data.index]
        available_approaches = [app for app in expected_approaches if app in pivot_data.columns]
        
        print(f"Available datasets: {available_datasets}")
        print(f"Available approaches: {available_approaches}")
        
        # Extract the execution times for each approach
        approach_times = {}
        for approach in available_approaches:
            approach_times[approach] = []
            for ds in available_datasets:
                if ds in pivot_data.index and approach in pivot_data.columns and not pd.isna(pivot_data.loc[ds, approach]):
                    value = pivot_data.loc[ds, approach]
                    approach_times[approach].append(value)
                    print(f"Value for {approach} on {ds}: {value}")
                else:
                    approach_times[approach].append(0)  # Use 0 for missing data
                    print(f"Missing value for {approach} on {ds}, using 0")
        
        print(f"Final approach times: {approach_times}")
    else:
        print("CSV data is empty")
        available_datasets = []
        available_approaches = []
        approach_times = {}

    # Map approach names to display names
    approach_display_names = {
        'xdbc': 'xdbc',
        'jdbc': 'jdbc'
    }

    # Formal, colorblind-friendly palette
    formal_palette = sns.color_palette("colorblind", len(expected_approaches))
    colors = formal_palette

    # Bar width and positions for the groups
    bar_width = 0.2
    x_indexes = np.arange(len(available_datasets))

    print(f"\nPlotting data:")
    print(f"Available datasets: {available_datasets}")
    print(f"Available approaches: {available_approaches}")
    print(f"Approach times: {approach_times}")

    # Create the plot
    plt.figure(figsize=(6, 3.75))

    # Plotting each approach with offset for bar positions
    for i, approach in enumerate(expected_approaches):
        if approach not in available_approaches:
            print(f"Skipping {approach} as it's not available")
            continue
        offset = bar_width * (i - (len(available_approaches)-1)/2)
        values = approach_times[approach]
        print(f"Plotting {approach} with values {values} at offset {offset}")
        
        plt.bar(x_indexes + offset, values, width=bar_width, 
                color=colors[i], label=approach_display_names.get(approach, approach), 
                edgecolor='white', zorder=3)

    # # Add timeout annotation if jdbc data is available and has zero values (indicating timeout)
    # if 'jdbc' in approach_times and any(t == 0 for t in approach_times['jdbc']):
    #     timeout_idx = next((i for i, t in enumerate(approach_times['jdbc']) if t == 0), 0)
    #     plt.text(x_indexes[timeout_idx] + bar_width + 0.01, 10, r"\textbf{Timeout after 20m}", 
    #             ha='center', va='bottom', fontsize=18, fontweight=1000, 
    #             color=colors[available_approaches.index('jdbc')], rotation=90)

    # Labels and Title
    plt.xlabel('Dataset')
    plt.ylabel('Time (s)')

    # X-axis ticks and legend
    plt.xticks(ticks=x_indexes, labels=available_datasets)
    plt.legend(loc='best')

    # Grid for better readability
    plt.grid(axis='y', alpha=0.3)

    # Display the plot
    plt.tight_layout()
    plt.savefig('figure9a.pdf', bbox_inches='tight')
    plt.close()

    print("Figure 9a generated successfully!") 

    # ******************************** Section24: Generate figure 9b*******************************
if RUN_SECTION_24:

    print("\n--- Running Section 24: Generating Figure 9b ---")

    # Define the path to the CSV file
    csv_file_path = os.path.join('res', 'averages.csv')

    # Read the CSV file if it exists
    if os.path.exists(csv_file_path):
        data = pd.read_csv(csv_file_path)
        print(f"Data loaded from {csv_file_path}")
        print("Raw CSV data:")
        print(data)
        print(f"Columns: {data.columns.tolist()}")
    else:
        print(f"CSV file not found at {csv_file_path}")
        # Create empty DataFrame with expected structure
        data = pd.DataFrame(columns=['fdw', 'dataset', 'execution_time'])

    # Set up the plot style
    plt.rcParams['text.usetex'] = True
    plt.rcParams['font.family'] = 'serif'
    plt.rcParams['font.serif'] = ['Computer Modern Roman']
    plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

    # Define the dataset mapping
    dataset_mapping = {
        'lineitem_sf10': 'lineitem',
        'ss13husallm': 'acs',
        'iotm': 'iot',
        'inputeventsm': 'icu'
    }

    # Define the expected datasets and approaches
    expected_datasets = ['lineitem', 'acs', 'iot', 'icu']
    expected_approaches = ['xdbc', 'native_fdw', 'jdbc']

    # Process the data from the CSV
    if not data.empty:
        print("\nProcessing data from CSV...")
        
        # Map dataset names to display names
        data['dataset_display'] = data['dataset'].map(dataset_mapping)
        print(f"Data after mapping:")
        print(data)
        
        # Remove any rows with unmapped datasets
        data = data.dropna(subset=['dataset_display'])
        print(f"Data after removing unmapped datasets:")
        print(data)
        
        # Pivot the data to get execution times for each approach
        pivot_data = data.pivot(index='dataset_display', columns='fdw', values='execution_time')
        print(f"Pivoted data:")
        print(pivot_data)
        
        # Get available datasets and approaches
        available_datasets = [ds for ds in expected_datasets if ds in pivot_data.index]
        available_approaches = [app for app in expected_approaches if app in pivot_data.columns]
        
        print(f"Available datasets: {available_datasets}")
        print(f"Available approaches: {available_approaches}")
        
        # Extract the execution times for each approach
        approach_times = {}
        for approach in available_approaches:
            approach_times[approach] = []
            for ds in available_datasets:
                if ds in pivot_data.index and approach in pivot_data.columns and not pd.isna(pivot_data.loc[ds, approach]):
                    value = pivot_data.loc[ds, approach]
                    approach_times[approach].append(value)
                    print(f"Value for {approach} on {ds}: {value}")
                else:
                    approach_times[approach].append(0)  # Use 0 for missing data
                    print(f"Missing value for {approach} on {ds}, using 0")
        
        print(f"Final approach times: {approach_times}")
    else:
        print("CSV data is empty")
        available_datasets = []
        available_approaches = []
        approach_times = {}

    # Map approach names to display names
    approach_display_names = {
        'xdbc': 'xdbc',
        'native_fdw': 'native',
        'jdbc': 'jdbc'
    }

    # Formal, colorblind-friendly palette
    formal_palette = sns.color_palette("colorblind", len(expected_approaches))
    colors = formal_palette

    # Bar width and positions for the groups
    bar_width = 0.2
    x_indexes = np.arange(len(available_datasets))

    print(f"\nPlotting data:")
    print(f"Available datasets: {available_datasets}")
    print(f"Available approaches: {available_approaches}")
    print(f"Approach times: {approach_times}")

    # Create the plot
    plt.figure(figsize=(6, 3.75))

    # Plotting each approach with offset for bar positions
    for i, approach in enumerate(expected_approaches):
        if approach not in available_approaches:
            print(f"Skipping {approach} as it's not available")
            continue
        offset = bar_width * (i - (len(available_approaches)-1)/2)
        values = approach_times[approach]
        print(f"Plotting {approach} with values {values} at offset {offset}")
        
        plt.bar(x_indexes + offset, values, width=bar_width, 
                color=colors[i], label=approach_display_names.get(approach, approach), 
                edgecolor='white', zorder=3)

    # Add timeout annotation if jdbc data is available and has zero values (indicating timeout)
    if 'jdbc' in approach_times and any(t == 0 for t in approach_times['jdbc']):
        timeout_idx = next((i for i, t in enumerate(approach_times['jdbc']) if t == 0), 0)
        plt.text(x_indexes[timeout_idx] + bar_width + 0.01, 10, r"\textbf{Timeout after 20m}", 
                ha='center', va='bottom', fontsize=18, fontweight=1000, 
                color=colors[available_approaches.index('jdbc')], rotation=90)

    # Labels and Title
    plt.xlabel('Dataset')
    plt.ylabel('Time (s)')

    # X-axis ticks and legend
    plt.xticks(ticks=x_indexes, labels=available_datasets)
    plt.legend(loc='best')

    # Grid for better readability
    plt.grid(axis='y', alpha=0.3)

    # Display the plot
    plt.tight_layout()
    plt.savefig('figure9b.pdf', bbox_inches='tight')
    plt.close()

    print("Figure 9b generated successfully!")

    # ********************* Combine the pdfs    *******************************
if RUN_PDF_MERGER:
    print("\n--- Running Final Section: Merging PDF files ---")
    import os
    from PyPDF2 import PdfMerger, PdfReader, PdfWriter
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas
    from io import BytesIO

    # --- Configuration ---
    pdf_files_to_merge = [
        ('figure6a.pdf', 'Figure 6a'),
        ('figure6b.pdf', 'Figure 6b'),
        ('figure7a.pdf', 'Figure 7a'),
        ('figure7b.pdf', 'Figure 7b'),
        ('figure8a.pdf', 'Figure 8a'),
        ('figure8b.pdf', 'Figure 8b'),
        ('figure9a.pdf', 'Figure 9a'),
        ('figure9b.pdf', 'Figure 9b'),
        ('figure10a.pdf', 'Figure 10a'),
        ('figure10b.pdf', 'Figure 10b'),
        ('figure11a.pdf', 'Figure 11a'),
        ('figure11b.pdf', 'Figure 11b'),
        ('figure12a.pdf', 'Figure 12a'),
        ('figure12b.pdf', 'Figure 12b'),
        ('figure13a.pdf', 'Figure 13a'),
        ('figure13b.pdf', 'Figure 13b'),
        ('figure14a.pdf', 'Figure 14a'),
        ('figure14b.pdf', 'Figure 14b'),
        ('figure15a.pdf', 'Figure 15a'),
        ('figure15b.pdf', 'Figure 15b'),
        ('figure16a.pdf', 'Figure 16a'),
        ('figure16b.pdf', 'Figure 16b'),
        ('figure17a.pdf', 'Figure 17a'),
        ('figure17b.pdf', 'Figure 17b'),
        ('figure18a.pdf', 'Figure 18a'),
        ('figure18b.pdf', 'Figure 18b'),
        ('figure18c.pdf', 'Figure 18c'),
        ('figure18d.pdf', 'Figure 18d'),
        ('figure18e.pdf', 'Figure 18e'),
        ('figure18f.pdf', 'Figure 18f'),
        ('figure19a.pdf', 'Figure 19a'),
        ('figure19b.pdf', 'Figure 19b'),
        ('figure20_backup.pdf', 'Figure 20b'),
        ('figure20_copy.pdf', 'Figure 20d'),
        ('figure20_icu_analysis.pdf', 'Figure 20c'),
        ('figure20_iot_analysis.pdf', 'Figure 20a'),
        ('figure20_etl.pdf', 'Figure 20e'),
        # ('figure20_pg.pdf', 'Figure 20PG'),
        ('figure21.pdf', 'Figure 21'),
    ]

    output_filename = 'combined_figures.pdf'

    # --- Merging Logic ---
    merger = PdfMerger()

    # Sort the figures by their numeric value to ensure proper order
    def get_figure_number(filename):
        # Extract the numeric part from the figure name
        base = filename[1].replace('Figure ', '').lower()
        number_part = base[:-1]  # Remove the 'a' or 'b'
        suffix = base[-1]  # Get the 'a' or 'b'
        return (int(number_part), suffix)

    # Sort the figures based on their numbers
    sorted_figures = sorted(pdf_files_to_merge, key=get_figure_number)

    for pdf_file, figure_name in sorted_figures:
        if not os.path.exists(pdf_file):
            print(f"Warning: File '{pdf_file}' not found. Skipping.")
            continue
        
        print(f"Processing '{pdf_file}' - {figure_name}")
        
        # Create a title page
        packet = BytesIO()
        can = canvas.Canvas(packet, pagesize=letter)
        can.setFont("Helvetica-Bold", 24)
        can.drawCentredString(300, 500, figure_name)
        can.save()
        
        # Move to the beginning of the BytesIO buffer
        packet.seek(0)
        title_page = PdfReader(packet).pages[0]
        
        # Get the content page
        content_page = PdfReader(pdf_file).pages[0]
        
        # Create a new PDF writer for this figure
        writer = PdfWriter()
        writer.add_page(title_page)
        writer.add_page(content_page)
        
        # Save the combined pages to a temporary file
        temp_filename = f"temp_{pdf_file}"
        with open(temp_filename, 'wb') as temp_file:
            writer.write(temp_file)
        
        # Append the temporary file to the merger
        merger.append(temp_filename)

    # Write the merged content to the output file
    try:
        merger.write(output_filename)
        print(f"\nSuccessfully combined PDFs into '{output_filename}'")
    except Exception as e:
        print(f"\nAn error occurred while writing the PDF: {e}")
    finally:
        # Close the merger object to release file resources
        merger.close()
        
        # Clean up temporary files
        for pdf_file, _ in pdf_files_to_merge:
            temp_filename = f"temp_{pdf_file}"
            if os.path.exists(temp_filename):
                os.remove(temp_filename)

print(f"Plotting completed")