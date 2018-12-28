import export_data
import os
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML
import base64
from io import BytesIO

export_choices = {
    1: ("1. Default dates table", "defaults_dates", ".csv", export_data.save_defaults_df),
    2: ("2. Defaults after 12 months table", "defaults_12_months", ".csv", export_data.save_defaults_12),
    3: ("3. Clients scoring table", "clients_scoring", ".csv", export_data.save_clients_scores),
    4: ("4. Age woe plot", "age_woe", ".png", export_data.save_age_of_car_woe_plot),
    5: ("5. Income woe plot", "income_woe", ".png", export_data.save_income_woe_plot),
    6: ("6. Age of car woe plot", "age_of_car_woe", ".png", export_data.save_age_of_car_woe_plot),
    0: ("0. Exit", ""),
}

while True:
    for choice_var in export_choices.values():
        print(choice_var[0])

    choice = input("Please choose what to export (Default: 1): ")

    try:
        choice = int(choice)
    except ValueError:
        choice = 1

    if choice == 0:
        exit(0)

    choice_info = export_choices.get(choice, 1)

    export_path = input("Please enter export path: ")

    while not os.path.exists(export_path):
        print("Path doesn't exist!")
        create_path = input("Create path? [Yes]: ")
        if not len(create_path) or create_path.lower() in ['yes', 'y', '1']:
            os.makedirs(export_path)
            print("Path created!")
            break
        else:
            export_path = input("Please enter export path: ")

    filename = input("Please enter file name [%s]: " % choice_info[1])
    filename = filename if len(filename) else choice_info[1]

    choice_info[3](os.path.join(export_path, filename + choice_info[2]))
    print("File saved to %s !" % os.path.join(export_path, filename + choice_info[2]))

    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template("my_report.html")

    risk_horizon = input("Please enter risk horizon in months [1-%d] (default 11): " % export_data.payments_df.age.max())

    try:
        risk_horizon = int(risk_horizon)
    except ValueError:
        risk_horizon = 11

    risk_horizon = max(1, min(export_data.payments_df.age.max(), risk_horizon))

    categorical_columns = ['gender',
                           'employed_by',
                           'education',
                           'marital_status',
                           'position',
                           'income_type',
                           'housing',
                           'house_ownership',
                           'children',
                           'family',
                           'age_category',
                           'income_category',
                           'age_of_car_category']

    columns_choice = {i + 1: categorical_columns[i] for i in range(len(categorical_columns))}
    for key in columns_choice.keys():
        print("%d. %s" % (key, columns_choice[key]))

    column_choice = input("Please choose analyzed column (Default 1): ")

    try:
        column_choice = int(column_choice)
    except ValueError:
        column_choice = 1

    woe_iv_df, woe_fig, iv_fig = export_data.get_woe_iv_risk_horizon(risk_horizon, columns_choice.get(column_choice, columns_choice[1]))

    export_path_pdf = input("Please enter report export path (Or leave blank to use old): ")

    if not len(export_path_pdf):
        export_path_pdf = export_path

    while not os.path.exists(export_path_pdf):
        print("Path doesn't exist!")
        create_path = input("Create path? [Yes]: ")
        if not len(create_path) or create_path.lower() in ['yes', 'y', '1']:
            os.makedirs(export_path_pdf)
            print("Path created!")
            break
        else:
            export_path_pdf = input("Please enter export path: ")

    filename_pdf = input("Please enter file name [%s]: " % "report")
    filename_pdf = filename_pdf if len(filename_pdf) else 'report'

    tmpfile = BytesIO()
    tmpfile1 = BytesIO()
    woe_fig.savefig(tmpfile, format='png')
    iv_fig.savefig(tmpfile1, format='png')

    encoded = base64.b64encode(tmpfile.getvalue())
    encoded1 = base64.b64encode(tmpfile1.getvalue())

    template_vars = {"title": "Отчет",
                     "column_name": columns_choice.get(column_choice, columns_choice[1]),
                     "risk_horizon": risk_horizon,
                     "woe_iv_df": woe_iv_df.to_html(),
                     'woe_plot': repr(encoded)[2:-1],
                     'iv_plot': repr(encoded1)[2:-1]}

    html_out = template.render(template_vars)

    HTML(string=html_out).write_pdf(os.path.join(export_path_pdf, filename_pdf + '.pdf'))
    print("Report saved to %s !" % (os.path.join(export_path_pdf, filename_pdf + '.pdf')))
