
import webbrowser
from pathlib import Path
import plotly.offline as py
import plotly.graph_objs as go


class HTMLReport:
    def __init__(self, header=None):
        self.mainframe = '''
            <html>
                <head>
                    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css">
                    <style>
                        h3{{ margin-top:25px; }}
                        body{{ margin:0 auto; width: 40%; background:white; }}
                        div{{ margin-top:50px; }}
                    </style>
                </head>
                <body>
                {components}
                </body>
            </html>
        '''
        self.components = ''
        if header is not None:
            self.components += f'<h3>{header}</h3>'

        self.plot_count = 0

    def add_table(self, df, header=None):
        self.components += '<div>'

        if header is not None:
            self.components += f'<h6>{header}</h6>'

        self.components += df.to_html(header=False, index=False)\
            .replace('<table border="1" class="dataframe">', '<table class="table">')

        self.components += '</div>'

    def add_chart(self, chart, header=None):
        self.components += '<div>'

        fig = go.Figure(data=chart, layout=go.Layout(showlegend=True, margin={'t': 0}))
        url = py.plot(fig, filename=f'report/plot_{self.plot_count}.html', auto_open=False)

        if header is not None:
            self.components += f'<h6>{header}</h6>'

        self.components += f'<iframe width="1000" height="500" frameborder="0" seamless="seamless" scrolling="no" src="{url}"></iframe>'
        self.plot_count += 1

        self.components += '</div>'

    def show(self):
        Path('report').mkdir(exist_ok=True)
        filename = 'report/report.html'
        with open(filename, 'w') as f:
            f.write(self.mainframe.format(components=self.components))
        webbrowser.open(url='file://' + str(Path(filename).absolute()), new=2)
